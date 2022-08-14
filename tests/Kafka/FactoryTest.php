<?php
declare(strict_types=1);

namespace Szemul\KafkaWorker\Test\Kafka;

use Mockery;
use Mockery\MockInterface;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use RdKafka\Conf;
use RdKafka\Exception as KafkaException;
use RdKafka\KafkaConsumer;
use RdKafka\TopicPartition;
use Szemul\KafkaWorker\Exception\RebalanceErrorException;
use Szemul\KafkaWorker\Kafka\Factory;
use Szemul\KafkaWorker\Test\Doubles\ConfDouble;

/** @covers \Szemul\KafkaWorker\Kafka\Factory */
class FactoryTest extends TestCase
{
    use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;

    protected Conf                          $conf;
    protected LoggerInterface|MockInterface $logger;
    protected Factory                       $sut;

    protected function setUp(): void
    {
        parent::setUp();

        $this->conf   = new Conf();
        $this->logger = Mockery::mock(LoggerInterface::class); // @phpstan-ignore-line

        $this->conf->set('group.id', 'test');

        $this->sut = new Factory($this->conf, $this->logger); // @phpstan-ignore-line
    }

    public function testGetConfig(): void
    {
        $this->assertSame($this->conf, $this->sut->getConfig());
    }

    public function testGetLogger(): void
    {
        $this->assertSame($this->logger, $this->sut->getLogger());
    }

    public function testGetConsumerAsDynamicMember(): void
    {
        $this->expectNoConsumerIdMessageLogged();

        $this->sut->getConsumer();
    }

    public function testGetConsumerWithStaticMemberSupport(): void
    {
        $this->expectStaticMemberRegistrationLogged('test');

        $this->sut->getConsumer('test');
    }

    public function testGetConsumerWithoutStaticMemberSupport(): void
    {
        $this->expectStaticMemberRegistrationFailedLogged();

        (new Factory(new ConfDouble(new KafkaException('test')), $this->logger))->getConsumer('test');
    }

    /** @dataProvider getCallbackValidatorMethods */
    public function testRebalanceCallback(string $validatorMethod): void
    {
        $conf = Mockery::mock(Conf::class);
        // @phpstan-ignore-next-line
        $conf->shouldReceive('setRebalanceCb')
            ->once()
            ->with(Mockery::on(fn (callable $callback) => $this->$validatorMethod($callback)));

        new Factory($conf, $this->logger); // @phpstan-ignore-line
    }

    /** @return array<string, array<string>> */
    public function getCallbackValidatorMethods(): array
    {
        return [
            'assignPartitions' => ['validateRebalanceCallbackAssignPartitions'],
            'revokePartitions' => ['validateRebalanceCallbackRevokePartitions'],
            'other'            => ['validateRebalanceCallbackOtherErrors'],
        ];
    }

    protected function validateRebalanceCallbackAssignPartitions(callable $callback): bool
    {
        $kafka     = $this->getKafkaConsumer();
        $partition = $this->getTopicPartition('test', 1, 2);
        $this->expectAssignedToPartitionsLogged();
        $kafka->shouldReceive('assign')->once()->with([$partition]); //@phpstan-ignore-line

        $callback($kafka, RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS, [$partition]);

        return true;
    }

    protected function validateRebalanceCallbackRevokePartitions(callable $callback): bool
    {
        $kafka = $this->getKafkaConsumer();
        $this->expectRevokedFromPartitionsLogged();
        $kafka->shouldReceive('assign')->once()->withNoArgs(); //@phpstan-ignore-line

        $callback($kafka, RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS);

        return true;
    }

    protected function validateRebalanceCallbackOtherErrors(callable $callback): bool
    {
        $kafka = $this->getKafkaConsumer();

        try {
            $callback($kafka, RD_KAFKA_RESP_ERR__AUTHENTICATION);

            $this->fail('No exception was thrown');
        } catch (RebalanceErrorException $e) {
            $this->assertSame((string)RD_KAFKA_RESP_ERR__AUTHENTICATION, $e->getMessage());
        }

        return true;
    }

    protected function expectNoConsumerIdMessageLogged(): static
    {
        // @phpstan-ignore-next-line
        $this->logger->shouldReceive('info')->with('No consumer ID specified. Registering as a dynamic member');

        return $this;
    }

    protected function expectStaticMemberRegistrationLogged(string $consumerId): static
    {
        // @phpstan-ignore-next-line
        $this->logger->shouldReceive('info')->with('Registering as a static member with id ' . $consumerId);

        return $this;
    }

    protected function expectStaticMemberRegistrationFailedLogged(): static
    {
        // @phpstan-ignore-next-line
        $this->logger->shouldReceive('notice')
            ->with('Kafka group.instance.id conf is not supported. Registering as a dynamic member');

        return $this;
    }

    protected function expectAssignedToPartitionsLogged(): static
    {
        // @phpstan-ignore-next-line
        $this->logger->shouldReceive('info')
            ->with(Mockery::on(function (string $message) {
                $this->assertStringContainsString('Assigned to partitions: ', $message);

                return true;
            }));

        return $this;
    }

    protected function expectRevokedFromPartitionsLogged(): static
    {
        // @phpstan-ignore-next-line
        $this->logger->shouldReceive('info')
            ->with('Revoked from all partitions');

        return $this;
    }

    protected function getKafkaConsumer(): KafkaConsumer|MockInterface
    {
        return Mockery::mock(KafkaConsumer::class); // @phpstan-ignore-line
    }

    protected function getTopicPartition(string $topic, int $partition, int $offset): TopicPartition|MockInterface
    {
        return new TopicPartition($topic, $partition, $offset);
    }
}
