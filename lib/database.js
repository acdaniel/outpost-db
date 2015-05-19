var util = require('util');
var EventEmitter = require('events').EventEmitter;

var Database = function (uri, options) {
  this._uri = uri;
  this._options = options;
  this._connectionStatus = 'disconnected';
  Object.defineProperty(this, 'connectionStatus', {
    enumerable: true,
    get: function () {
      return this._connectionStatus;
    }
  });

};
Database.prototype.connect = function () {};
Database.prototype.close = function () {};
Database.prototype.collection = function (name, options) {};
Database.prototype.createIndex = function (name, def, options) {};

util.inherits(Database, EventEmitter);

module.exports = Database;
