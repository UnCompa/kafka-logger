import { Kafka, Partitioners } from 'kafkajs';

interface CustomLog {
  ip?: string;
  appUser?: string;
  channel?: string;
  consumer?: string;
  amdocs360product?: string;
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
}

export class KafkaLogger {
  private producer;
  private topic: string; // Almacenar el tópico proporcionado

  constructor(brokers: string[], topic: string, clientId: string) {
    const kafka = new Kafka({
      clientId: clientId ?? 'logger-service',
      brokers: brokers,
    });

    this.topic = topic; // Asignar el tópico
    this.producer = kafka.producer({
      createPartitioner: Partitioners.LegacyPartitioner, // Usa el partitioner legado
    });
  }

  async connect() {
    try {
      await this.producer.connect();
      console.log('Kafka producer connected');
    } catch (error) {
      throw new Error('Error connecting Kafka producer' +  error);
    }
  }

  // Parámetro 'topic' opcional
  async logMessage(level: string, message: string, topic?: string) {
    if (!this.producer) {
      throw new Error('Producer is not connected');
    }

    try {
      await this.producer.send({
        topic: topic || this.topic, // Usar el tópico pasado o el del constructor
        messages: [{ key: level, value: message }],
      });
    } catch (error) {
      throw new Error('Failed to send log message to Kafka' + error);
    }
  }

  // Nuevo método para enviar el logEntry
  async logCustomMessage(customLog: CustomLog, topic?: string) {
    if (!this.producer) {
      throw new Error('Producer is not connected');
    }

    // Construir el logEntry con los valores por defecto
    const logEntry = {
      timestamp: new Date().toISOString(),
      componentType: "Backend",
      ip: customLog.ip || "172.20.102.187", // Cambia la IP según sea necesario
      appUser: customLog.appUser || "usrosbnewqabim",
      channel: customLog.channel || "web",
      consumer: customLog.consumer || "self service portal",
      amdocs360product: customLog.amdocs360product || "gg",
      apiName: customLog.apiName || "Nombre del api",
      microserviceName: customLog.microserviceName || "Nombre Microservicio",
      methodName: customLog.methodName || "Nombre del metodo ejecutado",
      layer: customLog.layer || "Exposicion",
      parentId: customLog.parentId || crypto.randomUUID(),
      referenceId: customLog.referenceId || crypto.randomUUID(),
      dateTimeTransacctionStart: customLog.dateTimeTransacctionStart || new Date().toISOString(),
      dateTimeTransacctionFinish: customLog.dateTimeTransacctionFinish || new Date().toISOString(),
      executionTime: customLog.executionTime || "tomar el tiempo de ejecución",
      country: customLog.country || "",
      city: customLog.city || "",
    };

    try {
      // Enviar el logEntry a Kafka
      await this.producer.send({
        topic: topic || this.topic, // Usar el tópico pasado o el del constructor
        messages: [{ value: JSON.stringify(logEntry) }],
      });
    } catch (error) {
      throw new Error('Failed to send custom log message to Kafka' + error);
    }
  }
}

