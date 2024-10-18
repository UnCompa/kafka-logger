import { Kafka, Partitioners } from 'kafkajs';

export interface CustomLog {
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

  constructor(brokers: string[], topic: string, clientId?: string) {
    const kafka = new Kafka({
      clientId: clientId ?? 'logger-service',
      brokers: brokers,
      retry: {
        retries: 5, // Número de reintentos
        initialRetryTime: 300, // Tiempo inicial entre reintentos
        factor: 2, // Factor de aumento del tiempo de espera entre reintentos
      },
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
      console.error('Error connecting Kafka producer:', error);
      setTimeout(this.connect, 5000);
      // Aquí puedes implementar lógica de reconexión o simplemente loguear el error
    }
  }

  // Parámetro 'topic' opcional
  async logMessage(level: string, message: string, topic?: string) {
    if (!this.producer) {
      console.error('Producer is not connected');
      // Intentar reconectar al productor
      await this.connect();
      return; // No continuar si no hay conexión
    }

    try {
      await this.producer.send({
        topic: topic || this.topic, // Usar el tópico pasado o el del constructor
        messages: [{ key: level, value: message }],
      });
    } catch (error) {
      console.error('Failed to send log message to Kafka:', error);
      // Aquí puedes implementar reintentos o algún mecanismo de fallback
    }
  }

  // Nuevo método para enviar el logEntry
  async logCustomMessage(level: string, customLog: CustomLog, topic?: string) {
    if (!this.producer) {
      console.error('Producer is not connected');
      // Intentar reconectar al productor
      await this.connect();
      return; // No continuar si no hay conexión
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
        messages: [{ key: level, value: JSON.stringify(logEntry) }],
      });
    } catch (error) {
      console.error('Failed to send custom log message to Kafka:', error);
      // Aquí también puedes implementar un mecanismo de reintento o fallback
    }
  }
}
