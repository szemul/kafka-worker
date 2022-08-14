<?php
declare(strict_types=1);

namespace Szemul\KafkaWorker\Test\Message;

use PHPUnit\Framework\TestCase;
use RdKafka\Message;
use Szemul\KafkaWorker\Exception\PayloadDecodingFailedException;
use Szemul\KafkaWorker\Message\KafkaMessage;

/** @covers \Szemul\KafkaWorker\Message\KafkaMessage */
class KafkaMessageTest extends TestCase
{
    protected const TOPIC_NAME = 'topic1';
    protected const OFFSET     = '5';
    protected const PAYLOAD    = ['foo' => 'bar'];

    public function testCreateWithInvalidPayload(): void
    {
        $message = $this->getMessage('foo');
        $this->expectException(PayloadDecodingFailedException::class);

        new KafkaMessage($message);
    }

    public function testCreateWithValidPayload(): void
    {
        $message = $this->getMessage();

        $sut = new KafkaMessage($message);

        $this->assertSame(self::PAYLOAD, $sut->getPayload());
        $this->assertSame(self::OFFSET, $sut->getQueueIdentifier());
        $this->assertSame(self::TOPIC_NAME, $sut->getJobName());
    }

    public function testSetQueueIdentifier(): void
    {
        $message = $this->getMessage();

        $sut = (new KafkaMessage($message))
            ->setQueueIdentifier('test');

        $this->assertSame(self::PAYLOAD, $sut->getPayload());
        $this->assertSame(self::TOPIC_NAME, $sut->getJobName());
        $this->assertSame('test', $sut->getQueueIdentifier());
    }

    protected function getMessage(?string $payload = null): Message
    {
        $message = new Message();

        $message->topic_name = self::TOPIC_NAME;
        $message->offset     = (int)self::OFFSET;
        $message->payload    = $payload ?? json_encode(self::PAYLOAD);

        return $message;
    }
}
