<?php
declare(strict_types=1);

namespace Szemul\KafkaWorker\Test\Worker;

use InvalidArgumentException;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use Mockery\MockInterface;
use PHPUnit\Framework\TestCase;
use RdKafka\KafkaConsumer;
use RdKafka\Message;
use RuntimeException;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Szemul\KafkaWorker\Exception\CommittableMessageException;
use Szemul\KafkaWorker\Exception\KafkaMessageException;
use Szemul\KafkaWorker\Kafka\Factory;
use Szemul\KafkaWorker\Message\KafkaMessage;
use Szemul\KafkaWorker\Worker\KafkaWorker;
use Szemul\QueueWorker\EventHandler\WorkerEventHandlerInterface;
use Szemul\QueueWorker\MessageProcessor\MessageProcessorInterface;
use Szemul\QueueWorker\Value\InterruptedValue;
use Throwable;

/** @covers \Szemul\KafkaWorker\Worker\KafkaWorker */
class KafkaWorkerTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    private const CONSUMER_ID        = 'consumer01';
    private const CONSUME_TIMEOUT_MS = KafkaWorker::DEFAULT_CONSUME_TIMEOUT_MS * 2;
    private const PAYLOAD            = ['foo' => 'bar'];
    private const TOPICS             = ['topic1', 'topic2'];
    private const MESSAGE_TOPIC      = self::TOPICS[0];
    private const MESSAGE_OFFSET     = 5;

    protected Factory|MockInterface                   $factory;
    protected MessageProcessorInterface|MockInterface $processor;
    protected InputInterface|MockInterface            $input;
    protected KafkaWorker                             $sut;
    protected InterruptedValue                        $interruptedValue;

    protected function setUp(): void
    {
        parent::setUp();

        $this->factory   = Mockery::mock(Factory::class); // @phpstan-ignore-line
        $this->processor = Mockery::mock(MessageProcessorInterface::class); // @phpstan-ignore-line
        $this->input     = Mockery::mock(InputInterface::class); // @phpstan-ignore-line

        $this->sut = $this->getSut();

        $this->interruptedValue = new InterruptedValue();
    }

    public function testConstructWithNoTopics_throwsException(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->getSut([]);
    }

    public function testGetFactory(): void
    {
        $this->assertSame($this->factory, $this->sut->getFactory());
    }

    public function testGetProcessor(): void
    {
        $this->assertSame($this->processor, $this->sut->getProcessor());
    }

    public function testGetConsumeTimeoutMs(): void
    {
        $this->assertSame(self::CONSUME_TIMEOUT_MS, $this->sut->getConsumeTimeoutMs());
    }

    public function testGetTopicsWithDefaults(): void
    {
        $this->assertSame(self::TOPICS, $this->sut->getTopics());
    }

    public function testGetTopicsWithCustom(): void
    {
        $topics = ['topic1'];
        $this->assertSame($topics, $this->getSut($topics)->getTopics());
    }

    public function testGetEventHandler(): void
    {
        $this->assertNull($this->sut->getEventHandler());
    }

    public function testSetEventHandler(): void
    {
        $eventHandler = $this->getEventHandler();
        $this->assertSame($this->sut, $this->sut->setEventHandler($eventHandler));
        $this->assertSame($eventHandler, $this->sut->getEventHandler());
    }

    public function testGetAdditionalInputDefinitions(): void
    {
        $result = $this->sut->getAdditionalInputDefinitions();

        $this->assertCount(1, $result);
        $this->assertContainsOnlyInstancesOf(InputOption::class, $result);
        $this->assertSame('consumer-id', $result[0]->getName());
        $this->assertSame('c', $result[0]->getShortcut());
        $this->assertTrue($result[0]->isValueRequired());
        $this->assertNull($result[0]->getDefault());
    }

    public function testWorkWithNoConsumerId(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectConsumerIdRetrieved(null);

        $this->sut->setEventHandler($this->getEventHandler())->work($this->interruptedValue, $this->input);
    }

    public function testWorkWithPartitionEof(): void
    {
        $consumer = $this->expectConsumerRetrieved();
        $this->expectConsumerIdRetrieved()
            ->expectMessageRetrieved($consumer, $this->getMessage(RD_KAFKA_RESP_ERR__PARTITION_EOF));

        $this->sut->setEventHandler($this->getEventHandler())->work($this->interruptedValue, $this->input);
    }

    public function testWorkWithTimeOut(): void
    {
        $consumer = $this->expectConsumerRetrieved();
        $this->expectConsumerIdRetrieved()
            ->expectMessageRetrieved($consumer, $this->getMessage(RD_KAFKA_RESP_ERR__TIMED_OUT));

        $this->sut->setEventHandler($this->getEventHandler())->work($this->interruptedValue, $this->input);
    }

    public function testWorkWithInterrupted(): void
    {
        $consumer = $this->expectConsumerRetrieved();
        $message  = $this->getMessage();

        $this->expectConsumerIdRetrieved()
            ->expectMessageRetrieved($consumer, $message);

        $this->sut->setEventHandler($this->getEventHandler())
            ->work($this->interruptedValue->setInterrupted(true), $this->input);
    }

    public function testWorkWithSuccess(): void
    {
        $eventHandler = $this->getEventHandler();
        $consumer     = $this->expectConsumerRetrieved();
        $message      = $this->getMessage();

        $this->expectConsumerIdRetrieved()
            ->expectMessageRetrieved($consumer, $message)
            ->expectMessageProcessed()
            ->expectMessageCommitted($consumer, $message)
            ->expectMessageReceivedEvent($eventHandler)
            ->expectMessageProcessedEvent($eventHandler)
            ->expectWorkerFinallyEvent($eventHandler);

        $this->sut->setEventHandler($eventHandler)->work($this->interruptedValue, $this->input);
    }

    public function testWorkWithSuccessWithoutEventHandler(): void
    {
        $consumer     = $this->expectConsumerRetrieved();
        $message      = $this->getMessage();

        $this->expectConsumerIdRetrieved()
            ->expectMessageRetrieved($consumer, $message)
            ->expectMessageProcessed()
            ->expectMessageCommitted($consumer, $message);

        $this->sut->work($this->interruptedValue, $this->input);
    }

    public function testWorkWithException(): void
    {
        $eventHandler = $this->getEventHandler();
        $message      = $this->getMessage();
        $consumer     = $this->expectConsumerRetrieved();
        $exception    = new RuntimeException('Test');
        $this->expectExceptionObject($exception);

        $this->expectConsumerIdRetrieved()
            ->expectMessageRetrieved($consumer, $message)
            ->expectMessageProcessedWithException($exception)
            ->expectMessageReceivedEvent($eventHandler)
            ->expectWorkerExceptionEvent($eventHandler, $exception)
            ->expectWorkerFinallyEvent($eventHandler);

        $this->sut->setEventHandler($eventHandler)->work($this->interruptedValue, $this->input);
    }

    public function testWorkWithCommittableException(): void
    {
        $eventHandler = $this->getEventHandler();
        $message      = $this->getMessage();
        $consumer     = $this->expectConsumerRetrieved();
        $exception    = new class ('Test') extends RuntimeException implements CommittableMessageException {};
        $this->expectExceptionObject($exception);

        $this->expectConsumerIdRetrieved()
            ->expectMessageRetrieved($consumer, $message)
            ->expectMessageProcessedWithException($exception)
            ->expectMessageReceivedEvent($eventHandler)
            ->expectMessageCommitted($consumer, $message)
            ->expectWorkerExceptionEvent($eventHandler, $exception)
            ->expectWorkerFinallyEvent($eventHandler);

        $this->sut->setEventHandler($eventHandler)->work($this->interruptedValue, $this->input);
    }

    public function testWorkWithExceptionInConsumer(): void
    {
        $eventHandler = $this->getEventHandler();
        $consumer     = $this->expectConsumerRetrieved();
        $exception    = new RuntimeException('Test');
        $this->expectExceptionObject($exception);

        $this->expectConsumerIdRetrieved()
            ->expectMessageRetrievedWithException($consumer, $exception)
            ->expectWorkerExceptionEvent($eventHandler, $exception);

        $this->sut->setEventHandler($eventHandler)->work($this->interruptedValue, $this->input);
    }

    public function testWorkWithInvalidMEssage(): void
    {
        $eventHandler = $this->getEventHandler();
        $message      = $this->getMessage(RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART);
        $consumer     = $this->expectConsumerRetrieved();
        $this->expectException(KafkaMessageException::class);

        $this->expectConsumerIdRetrieved()
            ->expectMessageRetrieved($consumer, $message)
            ->expectWorkerKafkaMessageExceptionEvent($eventHandler);

        $this->sut->setEventHandler($eventHandler)->work($this->interruptedValue, $this->input);
    }

    protected function expectConsumerIdRetrieved(?string $consumerId = self::CONSUMER_ID): static
    {
        $this->input->shouldReceive('getOption')  // @phpstan-ignore-line
            ->once()
            ->with('consumer-id')
            ->andReturn($consumerId);

        return $this;
    }

    /** @param string[] $topics */
    protected function expectConsumerRetrieved(array $topics = self::TOPICS): KafkaConsumer|MockInterface
    {
        $consumer = Mockery::mock(KafkaConsumer::class);

        $consumer->shouldReceive('subscribe')->with($topics); // @phpstan-ignore-line

        // @phpstan-ignore-next-line
        $this->factory->shouldReceive('getConsumer')->with(self::CONSUMER_ID)->andReturn($consumer);

        return $consumer; // @phpstan-ignore-line
    }

    protected function expectMessageRetrieved(MockInterface|KafkaConsumer $consumer, ?Message $message): static
    {
        // @phpstan-ignore-next-line
        $consumer->shouldReceive('consume')
            ->once()
            ->with(self::CONSUME_TIMEOUT_MS)
            ->andReturn($message);

        return $this;
    }

    protected function expectMessageRetrievedWithException(MockInterface|KafkaConsumer $consumer, Throwable $exception): static
    {
        // @phpstan-ignore-next-line
        $consumer->shouldReceive('consume')
            ->once()
            ->with(self::CONSUME_TIMEOUT_MS)
            ->andThrow($exception);

        return $this;
    }

    protected function expectMessageProcessed(): static
    {
        // @phpstan-ignore-next-line
        $this->processor->shouldReceive('process')
            ->once()
            ->with(Mockery::on([$this, 'validateKafkaMessage']));

        return $this;
    }

    protected function expectMessageProcessedWithException(Throwable $exception): static
    {
        // @phpstan-ignore-next-line
        $this->processor->shouldReceive('process')
            ->once()
            ->with(Mockery::on([$this, 'validateKafkaMessage']))
            ->andThrow($exception);

        return $this;
    }

    protected function expectMessageCommitted(MockInterface|KafkaConsumer $consumer, Message $message): static
    {
        $consumer->shouldReceive('commit')->once()->with($message); // @phpstan-ignore-line

        return $this;
    }

    public function validateKafkaMessage(KafkaMessage $message): bool
    {
        $this->assertSame(self::MESSAGE_TOPIC, $message->getJobName());
        $this->assertSame(self::PAYLOAD, $message->getPayload());
        $this->assertSame((string)self::MESSAGE_OFFSET, $message->getQueueIdentifier());

        return true;
    }

    protected function expectMessageReceivedEvent(MockInterface $eventHandler): static
    {
        $eventHandler->shouldReceive('handleMessageReceived')->once()->with(Mockery::on([$this, 'validateKafkaMessage'])); // @phpstan-ignore-line

        return $this;
    }

    protected function expectMessageProcessedEvent(MockInterface $eventHandler): static
    {
        $eventHandler->shouldReceive('handleMessageProcessed')->once()->with(Mockery::on([$this, 'validateKafkaMessage'])); // @phpstan-ignore-line

        return $this;
    }

    protected function expectWorkerFinallyEvent(MockInterface $eventHandler): static
    {
        $eventHandler->shouldReceive('handleWorkerFinally')->once()->withNoArgs(); // @phpstan-ignore-line

        return $this;
    }

    protected function expectWorkerExceptionEvent(MockInterface $eventHandler, Throwable $exception): static
    {
        $eventHandler->shouldReceive('handleWorkerException')->once()->with($exception); // @phpstan-ignore-line

        return $this;
    }

    protected function expectWorkerKafkaMessageExceptionEvent(MockInterface $eventHandler): static
    {
        // @phpstan-ignore-next-line
        $eventHandler->shouldReceive('handleWorkerException')
            ->once()
            ->with(Mockery::type(KafkaMessageException::class));

        return $this;
    }

    protected function getEventHandler(): WorkerEventHandlerInterface|MockInterface
    {
        return Mockery::mock(WorkerEventHandlerInterface::class); // @phpstan-ignore-line
    }

    protected function getMessage(int $err = RD_KAFKA_RESP_ERR_NO_ERROR): Message
    {
        $message = new Message();

        $message->err        = $err;
        $message->payload    = json_encode(self::PAYLOAD);
        $message->topic_name = self::MESSAGE_TOPIC;
        $message->offset     = self::MESSAGE_OFFSET;

        return $message;
    }

    /** @param string[] $topics */
    protected function getSut(array $topics = self::TOPICS): KafkaWorker
    {
        return new KafkaWorker($this->factory, $topics, $this->processor, self::CONSUME_TIMEOUT_MS);
    }
}
