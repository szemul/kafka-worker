<?php
declare(strict_types=1);

namespace Szemul\KafkaWorker\Builder;

use Psr\Log\LoggerInterface;
use RdKafka\Conf;
use Szemul\Helper\DateHelper;
use Szemul\KafkaWorker\Kafka\Factory;
use Szemul\KafkaWorker\Worker\KafkaWorker;
use Szemul\QueueWorker\Command\WorkerCommand;
use Szemul\QueueWorker\EventHandler\CommandEventHandlerInterface;
use Szemul\QueueWorker\EventHandler\WorkerEventHandlerInterface;
use Szemul\QueueWorker\MessageProcessor\MessageProcessorInterface;
use Szemul\QueueWorker\SignalHandler\SignalHandlerInterface;
use Szemul\QueueWorker\Value\InterruptedValue;

class KafkaCommandBuilder
{
    protected ?WorkerEventHandlerInterface  $workerEventHandler  = null;
    protected ?CommandEventHandlerInterface $commandEventHandler = null;
    protected ?SignalHandlerInterface       $signalHandler       = null;
    protected int                           $consumeTimeoutMs    = KafkaWorker::DEFAULT_CONSUME_TIMEOUT_MS;

    public function __construct(protected DateHelper $dateHelper, protected InterruptedValue $interruptedValue)
    {
    }

    public function setWorkerEventHandler(WorkerEventHandlerInterface $workerEventHandler): static
    {
        $this->workerEventHandler = $workerEventHandler;

        return $this;
    }

    public function setCommandEventHandler(CommandEventHandlerInterface $commandEventHandler): static
    {
        $this->commandEventHandler = $commandEventHandler;

        return $this;
    }

    public function setCommandSignalHandler(SignalHandlerInterface $signalHandler): static
    {
        $this->signalHandler = $signalHandler;

        return $this;
    }

    public function setConsumeTimeoutMs(int $consumeTimeoutMs): static
    {
        $this->consumeTimeoutMs = $consumeTimeoutMs;

        return $this;
    }

    public function build(
        string $name,
        Conf $kafkaConfig,
        MessageProcessorInterface $processor,
        LoggerInterface $logger,
        string ...$topics,
    ): WorkerCommand {
        if (empty($topics)) {
            throw new \InvalidArgumentException('At least 1 topic must be defined');
        }

        $factory = new Factory($kafkaConfig, $logger);
        $worker  = (new KafkaWorker($factory, $topics, $processor, $this->consumeTimeoutMs))
            ->setEventHandler($this->workerEventHandler);

        return (new WorkerCommand($this->dateHelper, $this->interruptedValue, $worker, $name))
            ->setEventHandler($this->commandEventHandler)
            ->setSignalHandler($this->signalHandler);
    }
}
