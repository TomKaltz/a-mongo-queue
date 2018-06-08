const test = require('ava')
const { oncePromise } = require('./util')

const setup = require('./setup.js')
const mongoDbQueue = require('../')
const queueName = 'enqueue'

let db

test.before('setup db', async t => {
  db = await setup(queueName)
})

test('a single message enqueued should return a promise that resolves with an ObjectID', async t => {
  let q = await mongoDbQueue(db, queueName)
  let id = q.enqueue()
  t.is(id.constructor.name, 'Promise', 'returns a promise object')
  t.is(typeof id.then, 'function', 'returns a thanable')
  id.catch(t.fail)
  let id2 = await id
  t.is(id2.constructor.name, 'ObjectID')
})

test('multiple messages enqueued should return a promise that resolves with an Array<ObjectID>', async t => {
  let q = await mongoDbQueue(db, queueName)
  let idPromise = q.enqueue([{},{}])
    .catch(t.fail)
  t.is(idPromise.constructor.name, 'Promise', 'returns a promise object')
  t.is(typeof idPromise.then, 'function', 'returns a thanable')
  let ids = await idPromise
  t.is(ids.constructor.name, 'Array')
  t.is(ids.length, 2)
  t.is(ids[0].constructor.name, 'ObjectID')
  t.is(ids[1].constructor.name, 'ObjectID')
  t.is(ids.length, 2)
})

test('should accept empty payload', async t =>{
  let q = await mongoDbQueue(db, queueName)
  await t.notThrows(q.enqueue(null))
  await t.notThrows(q.dequeue(undefined))
  await t.notThrows(q.dequeue({}))
})

test('q should emit an event with an id upon successful enqueue: single message', async t =>{
  let q = await mongoDbQueue(db, queueName)
  let all = await Promise.all([
    oncePromise(q, 'enqueued'),
    q.enqueue(),
  ])
  t.is(all[0].constructor.name, 'ObjectID')
  t.is(all[0], all[1])
})

test('q should emit an event with an Array<ObjectID></id> upon successful enqueue: multiple messages', async t =>{
  let q = await mongoDbQueue(db, queueName)
  let all = await Promise.all([
    oncePromise(q, 'enqueued'),
    q.enqueue([{},{}]),
  ])
  t.is(all[0].constructor.name, 'Array')
  t.is(all[0], all[1])
  t.is(all[0][0].constructor.name, 'ObjectID')
  t.is(all[0][1].constructor.name, 'ObjectID')
  t.is(all[0].length, 2)
})

test.after('close the db conn', t => {
  db.close()
})