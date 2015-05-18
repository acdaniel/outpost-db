var util = require('util');
var fs = require('fs');
var q = require('q');
var sift = require('sift');
var bson = require('bson');
var _ = require('lodash');
var debug = require('debug')('outpost-db:memory');
var Database = require('./database');

var MemoryDatabase = function (uri, options) {
  _.defaults(options, { encoding: 'utf8' });
  Database.apply(this, arguments);
  this._data = null;
};

util.inherits(MemoryDatabase, Database);

MemoryDatabase.prototype.connect = function () {
  var deferred = q.defer();
  debug('connecting to database: ' + this._uri);
  this._connectionStatus = 'connecting';
  this.emit('connecting');
  if (this._uri) {
    fs.readFile(this._uri, this._options, function (err, data) {
      if (err) { return deferred.reject(err); }
      this._data = JSON.parse(data);
      this.emit('connect', this);
      debug('connected to database');
      this._connectionStatus = 'connected';
      return deferred.resolve(this);
    }.bind(this));
  } else {
    process.nextTick(function () {
      this._data = {};
      this.emit('connect', this);
      debug('connected to database');  
      this._connectionStatus = 'connected';
      return deferred.resolve(this);
    }.bind(this));
  }
  return deferred.promise;
};

MemoryDatabase.prototype.close = function () {
  var deferred = q.defer();
  if (this.connectionStatus === 'connected' && this._db) {
    fs.writeFile(this._uri, this._data, this._options, function (err) {
      if (err) { return deferred.reject(err); }
      this._data = null;
      this._uri = null;
      this.emit('close');
      return deferred.resolve();      
    }.bind(this));
  } else {
    deferred.resolve();
  }
  return deferred.promise;
};

MemoryDatabase.prototype.collection = function (name, options) {
  var deferred = q.defer();
  if (this.connectionStatus === 'connected') {
    var collection = new MemoryCollection(this, name);
    deferred.resolve(collection);
  } else {
    debug('waiting on connection before loading collection ' + name);
    this.on('connect', function () {
      var collection = new MemoryCollection(this, name);
      deferred.resolve(collection);
    }.bind(this));
  }
  return deferred.promise;
};

var MemoryCollection = function (db, name) {
  this._db = db;
  this._name = name;
};

MemoryCollection.prototype.insertOne = function (doc, options, callback) {
  if (!callback) {
    callback = options;
    options = {};
  }
  if (!doc._id) {
    doc._id = bson.ObjectId();
  }
  if (!this._db._data.hasOwnProperty(this._name)) {
    this._db._data[this._name] = {};
  }
  if (this._db._data[this._name].hasOwnProperty(doc._id)) {
    return callback(new Error('duplicate id'));
  }
  this._db._data[this._name][doc._id] = doc;
  return callback(null, doc); 
};

MemoryCollection.prototype.updateOne = function (filter, update, options, callback) {
  if (!callback) {
    callback = options;
    options = {};
  }
  if (!this._db._data.hasOwnProperty(this._name)) {
    this._db._data[this._name] = {};
  }
  var data = _.values(this._db._data[this._name]);
  var test = sift(filter);
  if (update._id) {
    delete update._id;
  }
  for (var i = 0, l = data.length; i < l; i++) {
    if (test(data[i])) {
      this._db._data[this._name][data[i]._id] = _.assign(data[i], update);
      return callback(null, data[i]);
    }
  }
};

MemoryCollection.prototype.deleteMany = function (filter, options, callback) {
  if (!callback) {
    callback = options;
    options = {};
  }
  var results = [];
  var data = _.values(this._db._data[this._name]);
  var test = sift(filter);
  for (var i = 0, l = data.length; i < l; i++) {
    if (test(data[i])) {
      results.push(this._db._data[this._name][data._id]);
      delete this._db._data[this._name][data._id];
    }
  }
  return callback(null, results);
};

MemoryCollection.prototype.deleteOne = function (filter, options, callback) {
  if (!callback) {
    callback = options;
    options = {};
  }
  var results = [];
  var data = _.values(this._db._data[this._name]);
  var test = sift(filter);
  for (var i = 0, l = data.length; i < l; i++) {
    if (test(data[i])) {
      delete this._db._data[this._name][data._id];
      return callback(null, this._db._data[this._name][data._id]);
    }
  }
  return callback(null);
};

MemoryCollection.prototype.find = function (query, options, callback) {
  if (!callback) {
    callback = options;
    options = {};
  }
  var results = [];
  var data = _.values(this._db._data[this._name]);
  var test = sift(query);
  _.defaults(options, { limit: 0, sort: null, skip: 0 });
  var sorter = function (sort) {
    var fn = null;
    var props = [];
    var orders = [];
    if (!sort) {
      fn = function (data) {
        return data;
      };
    } else {
      for (var i = 0, l = sort.length; i < l; i++) {
        props.push(sort[0]);
        orders.push(sort[1] || 1);
      }
      fn = function (data) {
        return _sortByOrder(data, props, orders);
      };
    }
    return fn;
  };
  for (var i = 0, l = data.length; i < l; i++) {
    if (test(data[i])) {
      results.push(_.clone(data[i]));
    }
  }
  results = sorter(options.sort)(results);
  if (options.limit || options.skip) {
    results = results.slice(options.skip, options.skip + options.limit);
  }
  return callback(null, results);
};

MemoryCollection.prototype.findOne = function (query, options, callback) {
  if (!callback) {
    callback = options;
    options = {};
  }
  this.find(query, options, function (err, results) {
    return callback(err, results ? results[0] : undefined);
  });
};

MemoryCollection.prototype.count = function (query, options, callback) {
  if (!callback) {
    callback = options;
    options = {};
  }
  this.find(query, options, function (err, results) {
    return callback(err, results ? results.length : 0);
  });
};


module.exports = MemoryDatabase;