var mongodb = require('mongodb')

var conStr = 'mongodb://127.0.0.1:27017/mongodb-queue'

module.exports = function(queueName) {
  return new Promise((resolve, reject)=>{
    mongodb.MongoClient.connect(conStr, function(err, db) {
      if (err) reject(err)
      db.collection(queueName).remove(function() {
        resolve(db)  
      })
    })
  })
}
