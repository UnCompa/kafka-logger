"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.KafkaLogger = void 0;
const kafkajs_1 = require("kafkajs");
class KafkaLogger {
    constructor(brokers, topic, clientId) {
        const kafka = new kafkajs_1.Kafka({
            clientId: clientId !== null && clientId !== void 0 ? clientId : "logger-service",
            brokers: brokers,
            retry: {
                retries: 2, // Número de reintentos
                initialRetryTime: 300, // Tiempo inicial entre reintentos
                factor: 3, // Factor de aumento del tiempo de espera entre reintentos
            },
        });
        this.topic = topic; // Asignar el tópico
        this.producer = kafka.producer({
            createPartitioner: kafkajs_1.Partitioners.LegacyPartitioner, // Usa el partitioner legado
        });
    }
    connect() {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                yield this.producer.connect();
                console.log("Kafka producer connected");
            }
            catch (error) {
                throw new kafkajs_1.KafkaJSError("Error connecting Kafka producer");
                // Aquí puedes implementar lógica de reconexión o simplemente loguear el error
            }
        });
    }
    // Parámetro 'topic' opcional
    logMessage(level, message, topic) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.producer) {
                yield this.connect();
                throw new kafkajs_1.KafkaJSError("Failed to send log message to Kafka");
            }
            try {
                yield this.producer.send({
                    topic: topic || this.topic, // Usar el tópico pasado o el del constructor
                    messages: [{ key: level, value: message }],
                });
            }
            catch (error) {
                throw new kafkajs_1.KafkaJSError("Failed to send log message to Kafka: " + error);
                // Aquí puedes implementar reintentos o algún mecanismo de fallback
            }
        });
    }
    // Nuevo método para enviar el logEntry
    logCustomMessage(level, customLog, topic) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.producer) {
                yield this.connect();
                throw new kafkajs_1.KafkaJSError("Failed to send custom log message to Kafka");
            }
            let logEntry;
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
                    dateTimeTransacctionStart: customLog.dateTimeTransacctionStart || new Date().toISOString(),
                    dateTimeTransacctionFinish: customLog.dateTimeTransacctionFinish || new Date().toISOString(),
                    executionTime: customLog.executionTime || "tomar el tiempo de ejecución",
                    country: customLog.country || "",
                    city: customLog.city || "",
                };
            }
            else {
                logEntry = customLog;
            }
            try {
                // Enviar el logEntry a Kafka
                yield this.producer.send({
                    topic: topic || this.topic, // Usar el tópico pasado o el del constructor
                    messages: [{ key: level, value: JSON.stringify(logEntry) }],
                });
            }
            catch (error) {
                throw new kafkajs_1.KafkaJSError("Failed to send custom log message to Kafka:", error);
                // Aquí también puedes implementar un mecanismo de reintento o fallback
            }
        });
    }
}
exports.KafkaLogger = KafkaLogger;
