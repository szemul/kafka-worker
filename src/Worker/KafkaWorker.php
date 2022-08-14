<?php
declare(strict_types=1);

namespace Szemul\KafkaWorker\Worker;

use InvalidArgumentException;
use RdKafka\KafkaConsumer;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Szemul\KafkaWorker\Exception\CommittableMessageException;
use Szemul\KafkaWorker\Exception\KafkaMessageException;
use Szemul\KafkaWorker\Kafka\Factory;
use Szemul\KafkaWorker\Message\KafkaMessage;
use Szemul\QueueWorker\EventHandler\WorkerEventHandlerInterface;
use Szemul\QueueWorker\MessageProcessor\MessageProcessorInterface;
use Szemul\QueueWorker\Value\InterruptedValue;
use Szemul\QueueWorker\Worker\WorkerInterface;
use Throwable;

class KafkaWorker implements WorkerInterface
{
    public const DEFAULT_CONSUME_TIMEOUT_MS = 5000;

    protected ?WorkerEventHandlerInterface $eventHandler = null;
    /** @var array<string,KafkaConsumer> */
    protected array $consumers = [];

    /** @param string[] $topics */
    public function __construct(
        protected Factory $factory,
        protected array $topics,
        protected MessageProcessorInterface $processor,
        protected int $consumeTimeoutMs = self::DEFAULT_CONSUME_TIMEOUT_MS,
    ) {
        if (empty($this->topics)) {
            throw new InvalidArgumentException('At least 1 topic must be defined');
        }
    }

    /**
     * @return array<string,mixed>|null
     * @codeCoverageIgnore
     */
    public function __debugInfo(): ?array
    {
        $consumers = [];

        foreach ($this->consumers as $consumerId => $consumer) {
            $consumers[$consumerId] = '** Instance of ' . get_class($consumer);
        }

        return [
            'consumers'        => $consumers,
            'factory'          => $this->factory,
            'topics'           => $this->topics,
            'processor'        => $this->processor,
            'consumeTimeoutMs' => $this->consumeTimeoutMs,
            'eventHandler'     => $this->eventHandler,
        ];
    }

    public function setEventHandler(?WorkerEventHandlerInterface $eventHandler): static
    {
        $this->eventHandler = $eventHandler;

        return $this;
    }

    public function getEventHandler(): ?WorkerEventHandlerInterface
    {
        return $this->eventHandler;
    }

    public function getProcessor(): MessageProcessorInterface
    {
        return $this->processor;
    }

    public function getFactory(): Factory
    {
        return $this->factory;
    }

    /** @return string[] */
    public function getTopics(): array
    {
        return $this->topics;
    }

    public function getConsumeTimeoutMs(): int
    {
        return $this->consumeTimeoutMs;
    }

    public function getAdditionalInputDefinitions(): array
    {
        return [
            new InputOption(
                'consumer-id',
                'c',
                InputOption::VALUE_REQUIRED,
                'The consumer ID to use for the kafka consumer group',
            ),
        ];
    }

    /** @throws Throwable */
    public function work(InterruptedValue $interruptedValue, InputInterface $input): void
    {
        $consumerId = $input->getOption('consumer-id');

        if (null === $consumerId) {
            throw new InvalidArgumentException('No consumer ID set for the kafka worker');
        }

        try {
            if (!isset($this->consumers[$consumerId])) {
                $this->consumers[$consumerId] = $this->factory->getConsumer($consumerId);
                $this->consumers[$consumerId]->subscribe($this->topics);
            }

            $consumer = $this->consumers[$consumerId];

            $message = $consumer->consume($this->consumeTimeoutMs);
        } catch (Throwable $e) {
            $this->eventHandler?->handleWorkerException($e);

            throw $e;
        }

        try {
            if ($interruptedValue->isInterrupted()) {
                return;
            }

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    // No error, continue processing
                    break;

                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    // No more messages, return
                    return;

                default:
                    throw new KafkaMessageException($message);
            }
        } catch (KafkaMessageException $e) {
            $this->eventHandler?->handleWorkerException($e);

            throw $e;
        }

        try {
            $kafkaMessage = new KafkaMessage($message);
            $this->eventHandler?->handleMessageReceived($kafkaMessage);

            $this->processor->process($kafkaMessage);

            $this->eventHandler?->handleMessageProcessed($kafkaMessage);
            $consumer->commit($message);
        } catch (CommittableMessageException $e) {
            $this->eventHandler->handleWorkerException($e);
            $consumer->commit($message);

            throw $e;
        } catch (Throwable $e) {
            $this->eventHandler?->handleWorkerException($e);

            throw $e;
        } finally {
            $this->eventHandler?->handleWorkerFinally();
        }
    }
}
