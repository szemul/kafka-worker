<?php
declare(strict_types=1);

namespace Szemul\KafkaWorker\Test\Builder;

use InvalidArgumentException;
use Mockery;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use RdKafka\Conf;
use Szemul\Helper\DateHelper;
use Szemul\KafkaWorker\Builder\KafkaCommandBuilder;
use Szemul\KafkaWorker\Worker\KafkaWorker;
use Szemul\QueueWorker\EventHandler\CommandEventHandlerInterface;
use Szemul\QueueWorker\EventHandler\WorkerEventHandlerInterface;
use Szemul\QueueWorker\MessageProcessor\MessageProcessorInterface;
use Szemul\QueueWorker\SignalHandler\SignalHandlerInterface;
use Szemul\QueueWorker\Value\InterruptedValue;

/**
 * @covers \Szemul\KafkaWorker\Builder\KafkaCommandBuilder
 */
class KafkaCommandBuilderTest extends TestCase
{
    use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;

    private const  NAME  = 'test:test';
    private const TOPICS = [
        'topic1',
        'topic2',
    ];

    private DateHelper                $dateHelper;
    private InterruptedValue          $interruptedValue;
    private Conf                      $config;
    private MessageProcessorInterface $messageProcessor;
    private LoggerInterface           $logger;
    private KafkaCommandBuilder       $sut;

    protected function setUp(): void
    {
        parent::setUp();

        $this->dateHelper       = Mockery::mock(DateHelper::class); // @phpstan-ignore-line
        $this->interruptedValue = Mockery::mock(InterruptedValue::class); // @phpstan-ignore-line
        $this->config           = Mockery::mock(Conf::class); // @phpstan-ignore-line
        $this->messageProcessor = Mockery::mock(MessageProcessorInterface::class); // @phpstan-ignore-line
        $this->logger           = Mockery::mock(LoggerInterface::class); // @phpstan-ignore-line

        $this->config->shouldReceive('setRebalanceCb')->withAnyArgs(); // @phpstan-ignore-line

        $this->sut              = new KafkaCommandBuilder($this->dateHelper, $this->interruptedValue);
    }

    public function testWithNoTopics(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $command = $this->sut->build(self::NAME, $this->config, $this->messageProcessor, $this->logger);
    }

    public function testWithNoOptionalValues(): void
    {
        $command = $this->sut->build(
            self::NAME,
            $this->config,
            $this->messageProcessor,
            $this->logger,
            ...self::TOPICS,
        );

        $this->assertSame(self::NAME, $command->getName());
        $this->assertNull($command->getEventHandler());
        $this->assertNull($command->getSignalHandler());
        $this->assertSame($this->dateHelper, $command->getDateHelper());
        $this->assertSame($this->interruptedValue, $command->getInterruptedValue());

        /** @var KafkaWorker $worker */
        $worker = $command->getWorker();

        $this->assertInstanceOf(KafkaWorker::class, $worker);
        $this->assertNull($worker->getEventHandler());
        $this->assertSame($this->messageProcessor, $worker->getProcessor());
        $this->assertSame(KafkaWorker::DEFAULT_CONSUME_TIMEOUT_MS, $worker->getConsumeTimeoutMs());
        $this->assertSame(self::TOPICS, $worker->getTopics());

        $factory = $worker->getFactory();
        $this->assertSame($this->logger, $factory->getLogger());
        $this->assertSame($this->config, $factory->getConfig());
    }

    public function testWithOptionalValues(): void
    {
        $topics = self::TOPICS;

        /** @var WorkerEventHandlerInterface $workerEventHandler */
        $workerEventHandler   = Mockery::mock(WorkerEventHandlerInterface::class);
        /** @var CommandEventHandlerInterface $commandEventHandler */
        $commandEventHandler  = Mockery::mock(CommandEventHandlerInterface::class);
        /** @var SignalHandlerInterface $commandSignalHandler */
        $commandSignalHandler = Mockery::mock(SignalHandlerInterface::class);
        $consumeTimeout       = KafkaWorker::DEFAULT_CONSUME_TIMEOUT_MS * 2;

        $command = $this->sut->setWorkerEventHandler($workerEventHandler)
            ->setCommandSignalHandler($commandSignalHandler)
            ->setCommandEventHandler($commandEventHandler)
            ->setConsumeTimeoutMs($consumeTimeout)
            ->build(self::NAME, $this->config, $this->messageProcessor, $this->logger, ...self::TOPICS);

        $this->assertSame(self::NAME, $command->getName());
        $this->assertSame($commandEventHandler, $command->getEventHandler());
        $this->assertSame($commandSignalHandler, $command->getSignalHandler());
        $this->assertSame($this->dateHelper, $command->getDateHelper());
        $this->assertSame($this->interruptedValue, $command->getInterruptedValue());

        /** @var KafkaWorker $worker */
        $worker = $command->getWorker();

        $this->assertInstanceOf(KafkaWorker::class, $worker);
        $this->assertSame($workerEventHandler, $worker->getEventHandler());
        $this->assertSame($this->messageProcessor, $worker->getProcessor());
        $this->assertSame($consumeTimeout, $worker->getConsumeTimeoutMs());
        $this->assertSame(self::TOPICS, $worker->getTopics());

        $factory = $worker->getFactory();
        $this->assertSame($this->logger, $factory->getLogger());
        $this->assertSame($this->config, $factory->getConfig());
    }
}
