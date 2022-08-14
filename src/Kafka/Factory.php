<?php
declare(strict_types=1);

namespace Szemul\KafkaWorker\Kafka;

use Psr\Log\LoggerInterface;
use RdKafka\Conf;
use RdKafka\Exception;
use RdKafka\KafkaConsumer;
use RdKafka\TopicPartition;
use Szemul\KafkaWorker\Exception\RebalanceErrorException;

class Factory
{
    public function __construct(
        private Conf $config,
        private ?LoggerInterface $logger = null,
    ) {
        $this->config->setRebalanceCb(function (KafkaConsumer $kafka, $err, array $partitions = null) {
            /** @var TopicPartition[] $partitions */
            switch ($err) {
                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    $this->logger?->info(
                        'Assigned to partitions: ' . json_encode(
                            array_map(
                                fn (TopicPartition $partition) => sprintf(
                                    '%s/%d:%d',
                                    $partition->getTopic(),
                                    $partition->getPartition(),
                                    $partition->getOffset(),
                                ),
                                $partitions,
                            ),
                            JSON_UNESCAPED_SLASHES,
                        ),
                    );
                    $kafka->assign($partitions);
                    break;

                case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                    $this->logger?->info('Revoked from all partitions');
                    $kafka->assign();
                    break;

                default:
                    throw new RebalanceErrorException((string)$err);
            }
        });
    }

    /**
     * @return array<string,mixed>|null
     * @codeCoverageIgnore
     */
    public function __debugInfo(): ?array
    {
        return [];
    }

    public function getConfig(): Conf
    {
        return $this->config;
    }

    public function getLogger(): LoggerInterface
    {
        return $this->logger;
    }

    public function getConsumer(?string $consumerId = null): KafkaConsumer
    {
        if (null !== $consumerId) {
            try {
                $this->config->set('group.instance.id', $consumerId);
                $this->logger?->info('Registering as a static member with id ' . $consumerId);
            } catch (Exception) {
                // Static consumers are not supported, log this as a notice and ignore the error
                $this->logger?->notice(
                    'Kafka group.instance.id conf is not supported. Registering as a dynamic member',
                );
            }
        } else {
            $this->logger?->info('No consumer ID specified. Registering as a dynamic member');
        }

        return new KafkaConsumer($this->config);
    }
}
