const test = require('ava')
var MockDate = require('mockdate')

const setup = require('./setup.js')
const mongoDbQueue = require('../')
const queueName = 'ack'

let db
let q

test.before('setup db', async t => {
  db = await setup(queueName)
  q = await mongoDbQueue(db, queueName)
})

test('returns a promise', t => {
  let res = q.ack('doesnotexist')
  res.catch(()=>{})
  t.is(res.constructor.name, 'Promise', 'returns a promise object')
  t.is(typeof res.then, 'function', 'returns a thanable')
})

test('rejects promise if the id is not ackable', async t => {
  await t.throws(q.ack('thisiddoesnotexist'))
})

test('add a message, dequeue it, then successfully ack, resolving same message id', async t => {
  await q.enqueue()
  let msg = await q.dequeue()
  t.deepEqual(msg.id, await q.ack(msg.id))
})

test('reject ack on an expired lock', async t => {
  await q.enqueue()
  let msg = await q.dequeue({lockDuration: 1})
  MockDate.set(Date.now() + 1100)
  await t.throws(q.ack(msg.id))
  MockDate.reset()
})

test('reject ack if the message was already acked', async t => {
  await q.enqueue()
  let msg = await q.dequeue()
  t.truthy(msg) 
  t.truthy(await q.ack(msg.id))
  await t.throws(q.ack(msg.id))
})

test.after('close the db conn', t => {
  db.close()
})