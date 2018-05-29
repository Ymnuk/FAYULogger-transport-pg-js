const Transport = require('fayulogger/Transport');
const {Pool, Client} = require('pg');
const clonedeep = require('lodash.clonedeep');

/**
 * Класс-транспорт для сохранения логов в БД PostgreSQL
 */
class PgTransport extends Transport {
    /**
     * Конструктор
     * @param {String} name Название транспорта
     * @param {Object} options Параметры транспорта
     */
    constructor(name, options) {
        super(name);

        this.__host = options && options.host ? options.host : 'localhost';
        this.__port = options && options.port ? options.port : 5432;
        this.__database = options && options.database ? options.database : 'simple';
        this.__login = options && options.login ? options.login : null;
        this.__password = options && options.password ? options.password : null;
        this.__schema = options && options.schema ? options.schema : 'logs';//Схема, в которой будут созданы таблицы для хранения логов
        this.__tablePrefix = options && options.tablePrefix ? options.tablePrefix : 'log_';//Префикс названий таблиц в БД
        this.__timeoutFlush = options && options.timeoutFlush ? options.timeoutFlush : 10000;//Периодичность сохранения событий (логов) в БД. По умолчанию каждые 10 секунд//Время, через которое будут сброшены логи в БД, если долго небыло появления событий (логов). По умолчанию при отсутствии логов более 10 секунд будет выполнен сброс

        this.__bulkCount = options && options.bulkCount ? options.bulkCount : 1;//Количество вставляемых записей в БД за одну транзакцию

        this.__pool = null;

        this.__debugLogs = [];//Записи отладки (для bulkInsert)
        this.__infoLogs = [];//Записи информации (для bulkInsert)
        this.__warnLogs = [];//Записи предупреждений (для bulkInsert)
        this.__severeLogs = [];//Записи серьезных предупреждений (для bulkInsert)
        this.__errorLogs = [];//Записи ошибок (для bulkInsert)
        this.__fatalLogs = [];//Записи фатальных ошибок (для bulkInsert)

        this.on('debug', msg => this.__onDebug(msg));
        this.on('info', msg => this.__onInfo(msg));
        this.on('warn', msg => this.__onWarn(msg));
        this.on('severe', msg => this.__onSevere(msg));
        this.on('error', msg => this.__onError(msg));
        this.on('fatal', msg => this.__onFatal(msg));

        this.__timeout = null;
    }

    /**
     * Установление подключения и создание структуры хранения логов (при необходимости)
     */
    async connect() {
        this.__pool = new Pool({
            user: this.__login,
            host: this.__host,
            port: this.__port,
            database: this.__database,
            password: this.__password,
        })
        await this.__pool.query(`CREATE SCHEMA IF NOT EXISTS ${this.__schema}`);

        await this.__pool.query(`CREATE TABLE IF NOT EXISTS ${this.__schema}.${this.__tablePrefix}debug (dtevent timestamp with time zone NOT NULL DEFAULT now(), module character varying(50) COLLATE pg_catalog."default" NOT NULL, data jsonb)`);

        await this.__pool.query(`CREATE TABLE IF NOT EXISTS ${this.__schema}.${this.__tablePrefix}info (dtevent timestamp with time zone NOT NULL DEFAULT now(), module character varying(50) COLLATE pg_catalog."default" NOT NULL, data jsonb)`);

        await this.__pool.query(`CREATE TABLE IF NOT EXISTS ${this.__schema}.${this.__tablePrefix}warn (dtevent timestamp with time zone NOT NULL DEFAULT now(), module character varying(50) COLLATE pg_catalog."default" NOT NULL, data jsonb)`);

        await this.__pool.query(`CREATE TABLE IF NOT EXISTS ${this.__schema}.${this.__tablePrefix}severe (dtevent timestamp with time zone NOT NULL DEFAULT now(), module character varying(50) COLLATE pg_catalog."default" NOT NULL, data jsonb)`);

        await this.__pool.query(`CREATE TABLE IF NOT EXISTS ${this.__schema}.${this.__tablePrefix}error (dtevent timestamp with time zone NOT NULL DEFAULT now(), module character varying(50) COLLATE pg_catalog."default" NOT NULL, data jsonb)`);

        await this.__pool.query(`CREATE TABLE IF NOT EXISTS ${this.__schema}.${this.__tablePrefix}fatal (dtevent timestamp with time zone NOT NULL DEFAULT now(), module character varying(50) COLLATE pg_catalog."default" NOT NULL, data jsonb)`);

        this.__timeout = setTimeout(() => {
            console.log('Settend timeout');
            this.__timeoutHandler()
        }, this.__timeoutFlush)
    }

    __prepareMessageForSave(msg) {
        let newMsg = clonedeep(msg);
        let name = newMsg.name;
        delete newMsg.name;
        let prep = {
            dt: new Date(),
            name: name,
            msg: newMsg
        }
        return prep;
    }

    /**
     * Событие debug
     * @param {Object} msg Сообщение
     */
    __onDebug(msg) {
        this.__debugLogs.push(this.__prepareMessageForSave(msg));
    }

    /**
     * Событие info
     * @param {Object} msg Сообщение
     */
    __onInfo(msg) {
        this.__infoLogs.push(this.__prepareMessageForSave(msg));
    }

    /**
     * Событие Warn
     * @param {Object} msg Сообщение
     */
    __onWarn(msg) {
        this.__warnLogs.push(this.__prepareMessageForSave(msg));
    }

    /**
     * Событие severe
     * @param {Object} msg Сообщение
     */
    __onSevere(msg) {
        this.__severeLogs.push(this.__prepareMessageForSave(msg));
    }

    /**
     * Событие Error
     * @param {Object} msg Сообщение
     */
    __onError(msg) {
        this.__errorLogs.push(this.__prepareMessageForSave(msg));
    }

    /**
     * Событие fatal
     * @param {Object} msg Сообщение
     */
    __onFatal(msg) {
        this.__fatalLogs.push(this.__prepareMessageForSave(msg));
    }

    /**
     * Сохранение данных по таймауту
     */
    __timeoutHandler() {
        console.log('timeoutHandler');
        Promise.all([
            this.__flushDebug(),
            this.__flushInfo(),
            this.__flushWarn(),
            this.__flushSevere(),
            this.__flushError(),
            this.__flushFatal()
        ]).then(() => {
            this.__timeout = setTimeout(() => {
                this.__timeoutHandler()
            }, this.__timeoutFlush)
        }).catch(err => {
            throw err;
        })
    }

    /**
     * 
     * @param {String} level Уровень логирования
     * @param {Array} values Значения
     */
    async __saveIntoDB(level, values) {
        //console.log(values);
        if(values.length > 0) {
            const client = await this.__pool.connect();
            try {
                const sql = `INSERT INTO ${this.__schema}.${this.__tablePrefix}${level} (dtevent, module, data) VALUES ($1, $2, $3)`;
                let promises = new Array(values.length);
                for(let i = 0; i < values.length; i++) {
                    promises[i] = await client.query(sql, [values[i].dt, values[i].name, values[i].msg])
                }
                await Promise.all(promises);
                await client.query('COMMIT');
            } catch(e) {
                await client.query('ROLLBACK');
                throw e;
            } finally {
                client.release();
            }
        }
    }

    /**
     * Сохранение сообщений Debug
     */
    async __flushDebug() {
        let debug = this.__debugLogs;
        this.__debugLogs = [];
        this.__saveIntoDB('debug', debug);
    }

    /**
     * Сохранение сообщений Info
     */
    async __flushInfo() {
        let info = this.__infoLogs;
        this.__infoLogs = [];
        this.__saveIntoDB('info', info);
    }

    /**
     * Сохранение сообщений Warn
     */
    async __flushWarn() {
        let warn = this.__warnLogs;
        this.__warnLogs = [];
        this.__saveIntoDB('warn', warn);
    }

    /**
     * Сохранение сообщений Severe
     */
    async __flushSevere() {
        let severe = this.__severeLogs;
        this.__severeLogs = [];
        this.__saveIntoDB('severe', severe);
    }

    /**
     * Сохранение сообщений Error
     */
    async __flushError() {
        let error = this.__errorLogs;
        this.__errorLogs = [];
        this.__saveIntoDB('error', error);
    }

    /**
     * Сохранение сообщений Fatal
     */
    async __flushFatal() {
        let fatal = this.__fatalLogs;
        this.__fatalLogs = [];
        this.__saveIntoDB('fatal', fatal);
    }

    close() {
        super.close();
        this.__pool.end();
        if(this.__timeout) {
            clearTimeout(this.__timeout);
            this.__timeout = null;
        }
    }
}

module.exports = PgTransport;