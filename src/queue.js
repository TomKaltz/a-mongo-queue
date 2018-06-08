const os = require('os')
const util = require('util')

function nowPlusMs(ms) {
  return new Date(Date.now() + ms)
}

module.exports = async function(mongoDbClient, name, opts) {
  let q = new Queue(mongoDbClient, name, opts)  
  await q.ensureIndexes()
  q.processor = this.processor = null
  return q
}

function Queue(mongoDbClient, name, opts) {
  if (!mongoDbClient) throw new Error('a-mongo-queue: must provide a mongodb.MongoClient')
  if (!name) throw new Error('a-mongo-queue: must provide a queue name')
  opts = opts || {}
  this.name = name
  this.workerId = `${os.hostname()}-${process.pid}-${Math.floor(Math.random() * (9999 - 0))}`
  this.col = mongoDbClient.collection(name)
  this.lockDuration = opts.lockDuration || 10000
  this.delay = opts.delay || 0
  this.maxRetries = opts.maxRetries || 5
  this.priority = opts.priority || 5
  this.processor = null

  return this
}

let EventEmitter = require('events').EventEmitter
util.inherits(Queue, EventEmitter)

Queue.prototype.ensureIndexes = async function() {
  // TODO: probably index on status and visible
  // TODO: also make a separate index for waiting jobs
  return new Promise((resolve, reject)=>{
    this.col.createIndex({ status: 1, visible: 1 }, function(err, indexname) {
      if (err) return reject(err)
      resolve(indexname)
    })
  })
}


Queue.prototype.enqueue = async function(payload, opts) {
  let self = this
  opts = opts || {}
  let delay = opts.delay || this.delay
  let visible = delay ? nowPlusMs(delay) : new Date()
  let maxRetries = opts.maxRetries || this.maxRetries
  let priority = opts.priority || this.priority
  const jobTemplate = {
    created: new Date(),
    status: 'waiting',
    priority,
    visible,
    tries: 0,
    maxRetries,
    progress: 0,
    lastStarted: null,
    lastFailed: null,
    lastFailReason: null,
    worker: null,
  }
  let msgs = []
  if (payload instanceof Array) {
    if (payload.length === 0) {
      throw new Error('Queue.enqueue(): Array payload length must be greater than 0')
    }
    payload.forEach(function(payload) {
      msgs.push({...jobTemplate, payload})
    })
  } else {
    msgs.push({...jobTemplate, payload})
  }
  let results = await this.col.insertMany(msgs)
  if (payload instanceof Array){
    self.emit('enqueued', results.insertedIds)
    return results.insertedIds
  }else{
    self.emit('enqueued', results.ops[0]._id)
    return results.ops[0]._id
  }
}

Queue.prototype.dequeue = async function(opts = {}) {
  opts = opts || {}
  let visibility = opts.lockDuration || this.lockDuration
  let query = {
    status: 'waiting',
    visible: { $lte: new Date() },
  }
  let sort = {
    priority: -1,
    _id: 1,
  }
  let update = {
    $inc: { tries: 1 },
    $set: {
      status: 'running',
      visible: nowPlusMs(visibility),
      lastStarted: new Date(),
      worker: this.workerId,
    },
  }
  let { value: msg } = await this.col.findOneAndUpdate(query, update, { sort: sort, returnOriginal: false })
  if (!msg) return undefined
  msg.id = msg._id
  return msg
}

Queue.prototype.get = function(id) {
  return this.col.findOne({_id: id})
}

Queue.prototype.touch = async function(id, opts = {}) {
  let visibility = opts.lockDuration || this.lockDuration
  let query = {
    _id: id,
    status: 'running',
    visible: { $gte: new Date() },
  }
  let update = {
    $set: {
      visible: nowPlusMs(visibility),
    },
  }
  let { modifiedCount } = await this.col.updateOne(query, update, { new: true })
  if (modifiedCount !== 1)
    throw new Error(`Queue.touch(): There is no running job with the id: ${id} or the job has expired.`)
  return id
}

Queue.prototype.ack = async function(id) {
  let query = {
    _id: id,
    visible: { $gte: new Date() },
    status: 'running',
  }
  let update = {
    $set: {
      status: 'success',
      //worker: null, // TODO: should we leave this here to document which worker did the processing?
      progress: 100,
    },
  }
  let { value } = await this.col.findOneAndUpdate(query, update, { returnOriginal: false })
  if (!value)  throw new Error(`Queue.ack(): There is no job with the id: ${id} or the job has already finished.`)
  return value._id
}

Queue.prototype.progress = async function(id, progress = 0) {
  if(isNaN(progress))
    throw new Error('Queue.progress(): Progress paramter must be a number between 0 and 100')
  let query = {
    _id: id,
    status: 'running',
    visible: { $gte: new Date() },
  }
  if (progress < 0) progress = 0
  if (progress > 100) progress = 100
  let update = {
    $set: {
      progress,
    },
  }
  let { value } = await this.col.findOneAndUpdate(query, update, { returnOriginal: false })
  if (!value)
    throw new Error(`Queue.progress(): There is no running job with the id: ${id} or the job has expired.`)
  return value._id
}

Queue.prototype.fail = async function(id, reason) {
  let query = {
    _id: id,
    visible: { $gte: new Date() },
    status: 'running',
  }
  let update = {
    $set: {
      //worker: null,
      lastFailed: new Date(),
      lastFailReason: reason,
    },
  }
  let msg = await this.col.findOne(query)
  if (!msg) 
    throw new Error(`Queue.fail(): There is no job with the id: ${id} or the job has already finished.`)
  if (msg.tries > msg.maxRetries) {
    update.$set.status = 'failed'
  } else {
    update.$set.status = 'waiting'
    update.$set.visible = new Date()
  }
  let res = await this.col.findOneAndUpdate(query, update, { returnOriginal: false })
  if (!res.value)
    throw new Error(`Queue.fail(): There is no job with the id: ${id} or the job has already finished.`)
  return res.value._id
}

Queue.prototype.processStalledJobs = async function() {
//   let retryQuery = {
//     visible: { $lte: new Date() },
//     status: 'running',
//   }
}

Queue.prototype.cleanup = async function() {
  var query = {
    status: { $not: 'running' },
  }
  return this.col.deleteMany(query)
}

Queue.prototype.getProcessor = function() {
  return this.processor ? this.processor : undefined
}

Queue.prototype.process = function(fn, opts = {}) {
  if(this.processor)
    throw new Error('Queue.process(): there is already a processor initiated for this queue object')
  this.processor = new require('./processor')(this, fn, opts)
}

Queue.prototype.stop = function(graceful) {
  if(this.processor) {
    this.processor.stop(graceful)
  } else {
    throw new Error('Queue.stop(): there is no procesor defined')
  }
  return true
}
