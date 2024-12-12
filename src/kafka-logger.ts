import { fromNodeProviderChain } from '@aws-sdk/credential-providers';
import { Kafka, logLevel, Partitioners } from "kafkajs";
export interface CustomLog {
  timestamp?: string;
  level?: string;
  message?: string;
  ip?: string;
  appUser?: string;
  channel?: string;
  consumer?: string;
  apiName?: string;
  microserviceName?: string;
  methodName?: string;
  layer?: string;
  parentId?: string;
  referenceId?: string;
  dateTimeTransacctionStart?: string;
  dateTimeTransacctionFinish?: string;
  executionTime?: string;
  country?: string;
  city?: string;
  componentType?: string;
}

// Extiende los tipos de SASLOptions para incluir aws_msk_iam
type ExtendedSASLOptions =
  | {
    mechanism: 'plain' | 'scram-sha-256' | 'scram-sha-512' | 'aws' | 'oauthbearer';
    username: string;
    password: string;
  }
  | {
    mechanism: 'aws_msk_iam';
    authenticationProvider: () => Promise<{ username: string; password: string }>;
  };

export class KafkaLogger {
  private producer;
  private topic: string;

  constructor(brokers: string[], topic: string, clientId?: string) {
    const useAWSMSK = process.env.USE_AWS_MSK === 'true' || false;
    const msj = useAWSMSK ? 'Usign AWS mode' : "Usign default mode"
    console.info(msj)
    // Construye el objeto SASL dinámicamente si se usa AWS MSK
    const sasl: ExtendedSASLOptions | undefined = useAWSMSK
      ? {
        mechanism: 'aws_msk_iam',
        authenticationProvider: async () => {
          const credentials = await fromNodeProviderChain()();
          const accessKeyId = credentials.accessKeyId;
          const secretAccessKey = credentials.secretAccessKey;
          const sessionToken = credentials.sessionToken || '';
          const signature = Buffer.from(secretAccessKey, 'utf-8').toString(
            'base64',
          );

          return {
            username: `AWS:${accessKeyId}`,
            password: `${signature}${sessionToken ? `:${sessionToken}` : ''}`,
          };
        },
      }
      : undefined;

    const kafka = new Kafka({
      clientId: clientId ?? 'logger-service',
      brokers,
      ssl: useAWSMSK, // Habilita SSL solo si se usa AWS MSK
      sasl: sasl as any, // Asegúrate de pasar el tipo extendido
      logLevel: logLevel.INFO,
      retry: {
        retries: 5,
        initialRetryTime: 300,
        factor: 2,
      },
    });

    this.topic = topic;
    this.producer = kafka.producer({
      createPartitioner: Partitioners.LegacyPartitioner,
    });
  }

  async connect() {
    try {
      await this.producer.connect();
      console.info('[KAFKA MM] - Kafka producer connected');
    } catch (error) {
      console.error('[KAFKA MM] - Error connecting Kafka producer:', error.message);
      setTimeout(() => this.connect(), 5000); // Reintentar conexión
    }
  }

  async logMessage(level: string, message: string, topic?: string) {
    if (!this.producer) {
      console.error('[KAFKA MM] - Producer is not connected');
      await this.connect();
      return;
    }

    try {
      await this.producer.send({
        topic: topic || this.topic,
        messages: [{ key: level, value: message }],
      });
    } catch (error) {
      console.error('[KAFKA MM] - Failed to send log message to Kafka:', error.message);
    }
  }

  async logCustomMessage(level: string, customLog: object, topic?: string) {
    if (!this.producer) {
      console.error('[KAFKA MM] - Producer is not connected');
      await this.connect();
      return;
    }

    try {
      await this.producer.send({
        topic: topic || this.topic,
        messages: [{ value: JSON.stringify({ level, ...customLog }) }],
      });
    } catch (error) {
      console.error(
        '[KAFKA MM] - Failed to send custom log message to Kafka:',
        error.message,
      );
    }
  }
}