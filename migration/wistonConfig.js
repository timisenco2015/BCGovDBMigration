const winston = require('winston');
const config = require('config');

module.exports = winston.createLogger({
  level: 'info',
  format: winston.format.json(),
  transports: [
    //
    // - Write all logs with importance level of `error` or less to `error.log`
    // - Write all logs with importance level of `info` or less to `combined.log`
    //
    new winston.transports.File({ filename: config.get('logs.errorLogLocation'), level: 'error' }),
    new winston.transports.File({ filename: config.get('logs.otherLogLocation'), level: 'info' }),
    new winston.transports.Console({ filename: config.get('logs.otherLogLocation'), level: 'info' }),
  ],
});

//
// If we're not in production then log to the `console` with the format:
// `${info.level}: ${info.message} JSON.stringify({ ...rest }) `
//
/*if (process.env.NODE_ENV !== 'production') {
  logger.add(new winston.transports.Console({
    format: winston.format.simple(),
  }));
  
}
*/