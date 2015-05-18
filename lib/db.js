var MongoDatabase = require('./mongodb');

exports.connect = function (uri, options) {
  var db = new MongoDatabase(uri, options);
  db.connect();
  return db;
};