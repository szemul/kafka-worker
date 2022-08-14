<?php
declare(strict_types=1);

namespace Szemul\KafkaWorker\Test\Exception;

use Mockery;
use PHPUnit\Framework\TestCase;
use RdKafka\Message;
use Szemul\KafkaWorker\Exception\KafkaMessageException;

class KafkaMessageExceptionTest extends TestCase
{
    public function testFunctionality(): void
    {
        $message = $this->getMessage('test', RD_KAFKA_RESP_ERR__AUTHENTICATION);

        $sut = new KafkaMessageException($message);

        $this->assertSame('Error received in kafka message: test', $sut->getMessage());
        $this->assertSame(RD_KAFKA_RESP_ERR__AUTHENTICATION, $sut->getCode());
        $this->assertSame($message, $sut->getKafkaMessage());
    }

    protected function getMessage(string $errStr, int $code): Message
    {
        $mock = Mockery::mock(Message::class);

        // @phpstan-ignore-next-line
        $mock->shouldReceive('errstr')
            ->once()
            ->withNoArgs()
            ->andReturn($errStr);

        // @phpstan-ignore-next-line
        $mock->err = $code;

        return $mock; // @phpstan-ignore-line
    }
}
