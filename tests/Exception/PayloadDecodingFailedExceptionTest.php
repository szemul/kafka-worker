<?php
declare(strict_types=1);

namespace Szemul\KafkaWorker\Test\Exception;

use JsonException;
use Mockery;
use PHPUnit\Framework\TestCase;
use RdKafka\Message;
use Szemul\KafkaWorker\Exception\PayloadDecodingFailedException;

class PayloadDecodingFailedExceptionTest extends TestCase
{
    public function testFunctionality(): void
    {
        $exception = new JsonException('test', 5);
        $message   = Mockery::mock(Message::class);

        $sut = new PayloadDecodingFailedException($message, $exception); // @phpstan-ignore-line

        $this->assertSame('Failed to decode message payload as JSON: test', $sut->getMessage());
        $this->assertSame(5, $sut->getCode());
        $this->assertSame($message, $sut->getKafkaMessage());
        $this->assertSame($exception, $sut->getPrevious());
    }
}
