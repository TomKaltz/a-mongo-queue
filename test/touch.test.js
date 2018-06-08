const test = require('ava')
const MockDate = require('mockdate')

const setup = require('./setup.js')
const mongoDbQueue = require('../')
const queueName = 'touch'

let db
let q

test.before('setup db', async t => {
  db = await setup(queueName)
  q = await mongoDbQueue(db, queueName)
})

test('returns a promise', t => {
  let res = q.touch('doesnotexist')
  res.catch(()=>{})
  t.is(res.constructor.name, 'Promise', 'returns a promise object')
  t.is(typeof res.then, 'function', 'returns a thanable')
})

test('rejects promise if the id is not touchable', async t => {
  await t.throws(q.touch('thisiddoesnotexist'))
})

test('add a message, dequeue it, then successfully touch, resolving message id', async t => {
  await q.enqueue()
  let msg = await q.dequeue()
  t.deepEqual(msg.id, await q.touch(msg.id))
})

test('reject promise on an expired lock', async t => {
  await q.enqueue()
  let msg = await q.dequeue({lockDuration: 1})
  MockDate.set(Date.now() + 1100)
  await t.throws(q.touch(msg.id))
  MockDate.reset()
})

test('reject promise on a message that was already acked', async t => {
  await q.enqueue()
  let msg = await q.dequeue()
  await q.ack(msg.id)
  await t.throws(q.touch(msg.id))
})

test.after('close the db conn', t => {
  db.close()
})