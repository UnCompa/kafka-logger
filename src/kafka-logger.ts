import { Kafka, KafkaJSError, Partitioners } from "kafkajs";

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

export class KafkaLogger {
  private producer;
  private topic: string; // Almacenar el tópico proporcionado

  constructor(brokers: string[], topic: string, clientId?: string) {
    const kafka = new Kafka({
      clientId: clientId ?? "logger-service",
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
      console.log("Kafka producer connected");
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
    let logEntry: CustomLog | string;
    // Construir el logEntry con los valores por defecto
    if (typeof customLog !== "string") {
      logEntry = {
        timestamp: new Date().toISOString(),
        level: customLog.level,
        message: customLog.message,
        componentType: "Backend",
        ip: customLog.ip || "172.20.102.187", // Cambia la IP según sea necesario
        appUser: customLog.appUser || "usrosbnewqabim",
        channel: customLog.channel || "web",
        consumer: customLog.consumer || "self service portal",
        apiName: customLog.apiName || "Nombre del api",
        microserviceName: customLog.microserviceName || "Nombre Microservicio",
        methodName: customLog.methodName || "Nombre del metodo ejecutado",
        layer: customLog.layer || "Exposicion",
        parentId: customLog.parentId,
        referenceId: customLog.referenceId,
        dateTimeTransacctionStart:
          customLog.dateTimeTransacctionStart || new Date().toISOString(),
        dateTimeTransacctionFinish:
          customLog.dateTimeTransacctionFinish || new Date().toISOString(),
        executionTime:
          customLog.executionTime || "tomar el tiempo de ejecución",
        country: customLog.country || "",
        city: customLog.city || "",
      };
    } else {
      logEntry = customLog;
    }
    try {
      // Enviar el logEntry a Kafka
      await this.producer.send({
        topic: topic || this.topic, // Usar el tópico pasado o el del constructor
        messages: [{ value: JSON.stringify(logEntry) }],
      });
    } catch (error) {
      console.error('Failed to send custom log message to Kafka:', error);
      // Aquí también puedes implementar un mecanismo de reintento o fallback
    }
  }
}
