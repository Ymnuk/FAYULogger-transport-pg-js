# Транспорт FAYULogger для PostgreSQL

## Использование

```javascript
const PgTransport = require('fayulogger-transport-pg');
const FAYULogger = require('fayulogger');

let logger = new FAYULogger();
let transport = new Transport('pg', {
    login: 'login',
    password: 'pwd',
    database: 'db'
})

logger.addModule('test');
logger.addTransport(transport);
logger.bind({
    module: 'test',
    transport: 'pg',
    level: 'debug'
})
```

# Внимание

    Модуль находится в разработке