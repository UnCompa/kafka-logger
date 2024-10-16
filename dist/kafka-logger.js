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
    constructor(brokers, topic) {
        const kafka = new kafkajs_1.Kafka({
            clientId: 'logger-service',
            brokers: brokers,
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
                console.log('Kafka producer connected');
            }
            catch (error) {
                console.error('Error connecting Kafka producer', error);
            }
        });
    }
    // Parámetro 'topic' opcional
    logMessage(level, message, topic) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.producer) {
                console.error('Producer is not connected');
                return;
            }
            try {
                yield this.producer.send({
                    topic: topic || this.topic,
                    messages: [{ key: level, value: message }],
                });
            }
            catch (error) {
                console.error('Failed to send log message to Kafka', error);
            }
        });
    }
    // Nuevo método para enviar el logEntry
    logCustomMessage(customLog, topic) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.producer) {
                console.error('Producer is not connected');
                return;
            }
            // Construir el logEntry con los valores por defecto
            const logEntry = {
                timestamp: new Date().toISOString(),
                componentType: "Backend",
                ip: customLog.ip || "172.20.102.187",
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
                yield this.producer.send({
                    topic: topic || this.topic,
                    messages: [{ value: JSON.stringify(logEntry) }],
                });
                console.log('Log sent to Kafka:', logEntry);
            }
            catch (error) {
                console.error('Failed to send custom log message to Kafka', error);
            }
        });
    }
}
exports.KafkaLogger = KafkaLogger;
