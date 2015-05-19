var util = require('util');
var q = require('q');
var debug = require('debug')('outpost-db:mongodb');
var MongoClient = require('mongodb').MongoClient;
var Database = require('./database');

var MongoDatabase = function (uri, options) {
  if (!/mongodb:\/\//.test(uri)) {
    throw new Error('invalid mongodb uri');
  }
  Database.apply(this, arguments);
  this._db = null;
};

util.inherits(MongoDatabase, Database);

MongoDatabase.prototype.connect = function () {
  var deferred = q.defer();
  debug('connecting to database: ' + this._uri);
  this._connectionStatus = 'connecting';
  this.emit('connecting');
  MongoClient.connect(this._uri, this._options, function (err, db) {
    if (err) { return deferred.reject(err); }
    this._db = db;
    this._connectionStatus = 'connected';
    this.emit('connect', this);
    debug('connected to database');
    return deferred.resolve(this);
  }.bind(this));
  return deferred.promise;
};

MongoDatabase.prototype.close = function () {
  var deferred = q.defer();
  if (this.connectionStatus === 'connected' && this._db) {
    this._db.close(function (err) {
      if (err) { return deferred.reject(err); }
      this._db = null;
      this.emit('close');
      return deferred.resolve();
    }.bind(this));
  } else {
    deferred.resolve();
  }
  return deferred.promise;
};

MongoDatabase.prototype.collection = function (name, options) {
  var deferred = q.defer();
  if (this.connectionStatus === 'connected') {
    debug('loading collection ' + name);
    this._db.collection(name, options, function (err, collection) {
      if (err) { return deferred.reject(err); }
      debug('collection ' + name + ' loaded');
      deferred.resolve(collection);
    });
  } else {
    debug('waiting on connection before loading collection ' + name);
    this.on('connect', function () {
      this._db.collection(name, options, function (err, collection) {
      if (err) { return deferred.reject(err); }
      debug('collection ' + name + ' loaded');
      deferred.resolve(collection);
    });
    }.bind(this));
  }
  return deferred.promise;
};

MongoDatabase.prototype.createIndex = function (name, def, options) {
  var deferred = q.defer();
  if (this.connectionStatus === 'connected') {
    debug('creating index ' + name);
    this._db.createIndex(name, def, options, function (err, indexName) {
      if (err) { return deferred.reject(err); }
      debug('index ' + indexName + ' created');
      deferred.resolve(indexName);
    });
  } else {
    debug('waiting on connection before creating index ' + name);
    this.on('connect', function () {
      this._db.createIndex(name, def, options, function (err, indexName) {
      if (err) { return deferred.reject(err); }
      debug('index ' + indexName + ' created');
      deferred.resolve(indexName);
    });
    }.bind(this));
  }
  return deferred.promise;
};

module.exports = MongoDatabase;
