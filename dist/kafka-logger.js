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
const credential_providers_1 = require("@aws-sdk/credential-providers");
const kafkajs_1 = require("kafkajs");
class KafkaLogger {
    constructor(brokers, topic, clientId) {
        const useAWSMSK = process.env.USE_AWS_MSK === 'true' || false;
        const msj = useAWSMSK ? 'Usign AWS mode' : "Usign default mode";
        console.info(msj);
        // Construye el objeto SASL dinámicamente si se usa AWS MSK
        const sasl = useAWSMSK
            ? {
                mechanism: 'aws_msk_iam',
                authenticationProvider: () => __awaiter(this, void 0, void 0, function* () {
                    const credentials = yield (0, credential_providers_1.fromNodeProviderChain)()();
                    const accessKeyId = credentials.accessKeyId;
                    const secretAccessKey = credentials.secretAccessKey;
                    const sessionToken = credentials.sessionToken || '';
                    const signature = Buffer.from(secretAccessKey, 'utf-8').toString('base64');
                    return {
                        username: `AWS:${accessKeyId}`,
                        password: `${signature}${sessionToken ? `:${sessionToken}` : ''}`,
                    };
                }),
            }
            : undefined;
        const kafka = new kafkajs_1.Kafka({
            clientId: clientId !== null && clientId !== void 0 ? clientId : 'logger-service',
            brokers,
            ssl: useAWSMSK, // Habilita SSL solo si se usa AWS MSK
            sasl: sasl, // Asegúrate de pasar el tipo extendido
            logLevel: kafkajs_1.logLevel.INFO,
            retry: {
                retries: 5,
                initialRetryTime: 300,
                factor: 2,
            },
        });
        this.topic = topic;
        this.producer = kafka.producer({
            createPartitioner: kafkajs_1.Partitioners.LegacyPartitioner,
        });
    }
    connect() {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                yield this.producer.connect();
                console.info('[KAFKA MM] - Kafka producer connected');
            }
            catch (error) {
                console.error('[KAFKA MM] - Error connecting Kafka producer:', error.message);
                setTimeout(() => this.connect(), 5000); // Reintentar conexión
            }
        });
    }
    logMessage(level, message, topic) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.producer) {
                console.error('[KAFKA MM] - Producer is not connected');
                yield this.connect();
                return;
            }
            try {
                yield this.producer.send({
                    topic: topic || this.topic,
                    messages: [{ key: level, value: message }],
                });
            }
            catch (error) {
                console.error('[KAFKA MM] - Failed to send log message to Kafka:', error.message);
            }
        });
    }
    logCustomMessage(level, customLog, topic) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.producer) {
                console.error('[KAFKA MM] - Producer is not connected');
                yield this.connect();
                return;
            }
            try {
                yield this.producer.send({
                    topic: topic || this.topic,
                    messages: [{ value: JSON.stringify(Object.assign({ level }, customLog)) }],
                });
            }
            catch (error) {
                console.error('[KAFKA MM] - Failed to send custom log message to Kafka:', error.message);
            }
        });
    }
}
exports.KafkaLogger = KafkaLogger;
