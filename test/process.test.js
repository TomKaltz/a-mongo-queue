const test = require('ava')
// var MockDate = require('mockdate')

const setup = require('./setup.js')
const mongoDbQueue = require('../')
// const queueName = 'process'

let db
// let q

// test.before('setup db', t => {
// db = await setup(queueName)
// q = await mongoDbQueue(db, queueName)
// })

// test('returns a promise', t => {
//   let res = q.process(q, ()=>{console.log('PASSED FN CALLED')})
//   // res.catch(()=>{})
//   t.is(res.constructor.name, 'Promise', 'returns a promise object')
//   t.is(typeof res.then, 'function', 'returns a thanable')
// })
test('multiple concurrency', async t => {
  db = await setup('process-concurrency')
  let q = await mongoDbQueue(db, 'process-concurrency')
  t.is(q.processor, null)
  let ids = await q.enqueue([{}, {}, {}, {}, {}, {}, {}, {}, {}])
  t.deepEqual(ids.constructor.name, 'Array')
  q.process(async (q, msg)=>{
    await new Promise(resolve => setTimeout(resolve, 200))
    return 'processor done'+msg.id
  },{concurrency: 5})
  // await new Promise(resolve => setTimeout(resolve, 8))
  t.is(q.processor.inFlight, 0)
  await new Promise(resolve => setTimeout(resolve, 100))
  t.is(q.processor.inFlight, 5)
  await new Promise(resolve => setTimeout(resolve, 250))
  t.is(q.processor.inFlight, 4)
  await new Promise(resolve => setTimeout(resolve, 500))
  t.is(q.processor.inFlight, 0)
  await q.enqueue([{}, {}, {}])
  q.processor.pollAndFillSlots()
  await new Promise(resolve => setTimeout(resolve, 150))
  t.is(q.processor.inFlight, 3)
  await new Promise(resolve => setTimeout(resolve, 75))
  t.is(q.processor.inFlight, 0)
})

test('no concurrency', async t => {
  db = await setup('process-single-concurrency')
  let q = await mongoDbQueue(db, 'process-single-concurrency')
  t.is(q.processor, null)
  let ids = await q.enqueue([{}, {}, {}])
  t.deepEqual(ids.constructor.name, 'Array')
  q.process(async (q, msg)=>{
    await new Promise(resolve => setTimeout(resolve, 200))
    return 'processor done'+msg.id
  },{ concurrency: 1 })
  await new Promise(resolve => setTimeout(resolve, 50))
  t.is(q.processor.inFlight, 1)
  await new Promise(resolve => setTimeout(resolve, 250))
  t.is(q.processor.inFlight, 1)
  await new Promise(resolve => setTimeout(resolve, 650))
  t.is(q.processor.inFlight, 0)
})

test('priority', async t => {
  db = await setup('process-priority')
  let q = await mongoDbQueue(db, 'process-priority')
  t.is(q.processor, null)
  let firstJob = await q.enqueue({},{priority:1})
  await new Promise(resolve => setTimeout(resolve, 50))
  let secondJob = await q.enqueue({},{priority:2})
  q.process(async (q, msg)=>{
    await new Promise(resolve => setTimeout(resolve, 4000))
    return 'processor done'+msg.id
  },{ concurrency: 1 })
  t.is((await q.get(firstJob)).status,'waiting')
  t.is((await q.get(secondJob)).status,'running')
})

test('process failure', async t => {
  db = await setup('process-failure')
  let q = await mongoDbQueue(db, 'process-failure')
  t.is(q.processor, null)
  let jobs = await q.enqueue([{},{}])
  q.process(async (q, msg)=>{
    // await new Promise(resolve => setTimeout(resolve, 4000))
    throw new Error('failed')
  })
  t.not(q.processor, null)
  t.is(typeof q.processor, 'object')
  t.is(q.processor.inFlight, 0)
  await new Promise(resolve => setTimeout(resolve, 200))
  t.is((await q.get(jobs[0])).status,'failed')
  t.is((await q.get(jobs[1])).status,'failed')
})

test.after('close the db conn', t => {
  db.close()
})