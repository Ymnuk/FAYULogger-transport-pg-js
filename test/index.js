const assert = require('assert');
const vows = require('vows');

const FAYULogger = require('fayulogger');
const PgTransport = require('../PgTransport');

const {Pool, Client} = require('pg');

let logger = null;

pgOptions = {
    database: 'logs',
    login: 'postgres',
    password: 'postgres'
}

let pool = null;

vows
    .describe('Main test')
        .addBatch({
            'Create logger': {
                topic: function() {
                    (async () => {
                        logger = new FAYULogger();
                        let pgTransport = new PgTransport('test', pgOptions)
                        await pgTransport.connect();
                        logger.addModule('test');
                        logger.addTransport(pgTransport);
                        logger.bind({
                            module: 'test',
                            transport: 'test',
                            level: 'debug'
                        });
                        return logger;
                    })().then(res => {
                        this.callback(null, res);
                    }).catch(err => {
                        console.log(err);
                        this.callback(err)
                    });
                },
                'Should be FAYULogger': function(err, res) {
                    if(err)
                        return assert.fail(err);
                    assert.ok(res instanceof FAYULogger);
                }
            }
        })
        .addBatch({
            'Verify send logs': {
                'Verify debug': {
                    topic: function() {
                        logger.getModule('test').debug({source: 'test', message: {text: 'debug'}});
                        return true;
                    },
                    'Is true': (res) => {
                        assert.ok(res)
                    }
                },
                'Verify info': {
                    topic: function() {
                        logger.getModule('test').info('test');
                        return true;
                    },
                    'Is true': (res) => {
                        assert.ok(res)
                    }
                },
                'Verify warn': {
                    topic: function() {
                        logger.getModule('test').warn('test');
                        return true;
                    },
                    'Is true': (res) => {
                        assert.ok(res)
                    }
                },
                'Verify severe': {
                    topic: function() {
                        logger.getModule('test').severe('test');
                        return true;
                    },
                    'Is true': (res) => {
                        assert.ok(res)
                    }
                },
                'Verify error': {
                    topic: function() {
                        logger.getModule('test').error('test');
                        return true;
                    },
                    'Is true': (res) => {
                        assert.ok(res)
                    }
                },
                'Verify fatal': {
                    topic: function() {
                        logger.getModule('test').fatal('test');
                        return true;
                    },
                    'Is true': (res) => {
                        assert.ok(res)
                    }
                }
            }
        })
        .addBatch({
            'Timeout for flush logs': {
                topic: function() {
                    setTimeout(() => {
                        this.callback(true)
                    }, 15000)
                },
                'Timeout OK': function(topic) {
                    assert.ok(topic);
                }
            }
        })
        .addBatch({
            'Close logger': {
                topic: function() {
                    return logger.free();
                },
                'Closed': function() {
                    assert.ok(true);
                }
            }
        })
        .addBatch({
            'Verify DB tables': {
                topic: function() {
                    (async () => {
                        pool = new Pool({
                            user: pgOptions.login,
                            password: pgOptions.password,
                            database: pgOptions.database
                        })
                        let resDebug = await pool.query('select count(*) as c from logs.log_debug');
                        let resInfo = await pool.query('select count(*) as c from logs.log_debug');
                        let resWarn = await pool.query('select count(*) as c from logs.log_debug');
                        let resSevere = await pool.query('select count(*) as c from logs.log_debug');
                        let resError = await pool.query('select count(*) as c from logs.log_debug');
                        let resFatal = await pool.query('select count(*) as c from logs.log_debug');
                        return [
                            +resDebug.rows[0].c,
                            +resInfo.rows[0].c,
                            +resWarn.rows[0].c,
                            +resSevere.rows[0].c,
                            +resError.rows[0].c,
                            +resFatal.rows[0].c,
                        ]
                    })().then(res => {
                        this.callback(null, res);
                    }).catch(this.callback)
                },
                'Verify count rows': function(err, res) {
                    //console.log(err);
                    //console.log(res);
                    if(err) return assert.fail(err);
                    assert.ok(res[0] === 1 && res[1] === 1 && res[2] === 1 && res[3] === 1 && res[4] === 1 && res[5] === 1);
                }
            }
        })
        .addBatch({
            'Clear tables': {
                topic: function() {
                    (async () => {
                        await pool.query('delete from logs.log_debug')
                        await pool.query('delete from logs.log_info')
                        await pool.query('delete from logs.log_warn')
                        await pool.query('delete from logs.log_severe')
                        await pool.query('delete from logs.log_error')
                        await pool.query('delete from logs.log_fatal')
                        await pool.end();
                    })().then(res => {
                        this.callback(null, res);
                    }).catch(this.callback);
                },
                'Clear OK': function() {
                    assert.ok(true);
                }
            }
        })
    .export(module);