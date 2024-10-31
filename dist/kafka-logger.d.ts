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
export declare class KafkaLogger {
    private producer;
    private topic;
    constructor(brokers: string[], topic: string, clientId?: string);
    connect(): Promise<void>;
    logMessage(level: string, message: string, topic?: string): Promise<void>;
    logCustomMessage(level: string, customLog: CustomLog, topic?: string): Promise<void>;
}
