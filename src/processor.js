const EventEmitter = require('events').EventEmitter

const Processor = function(q, fn, opts = {}){
  console.log('*****Processor Constructor*******')
  if(!q || q.constructor.name !== 'Queue') throw new Error('Processor: you must pass in a valid Queue object')
  if(!fn) throw new Error('Processor: you must pass in a function')

  const self = new EventEmitter()
  
  let pollInterval = opts.pollInterval || 2000
  let concurrency = opts.concurrency || 5
  self.inFlight = 0
  let pollTimer = null
  let stopped = false

  
  const clearWait = () => {
    if(pollTimer){
      console.log('clearing timeout')
      clearTimeout(pollTimer)
      pollTimer = null
    }
  }
  
  const waitThenPoll = () => {
    clearWait()
    console.log('waiting', pollInterval, self.inFlight)
    pollTimer = setTimeout(self.pollAndFillSlots, pollInterval)
  }

  self.pollAndFillSlots = () => {
    console.log('pollAndFillSlots called', self.inFlight)
    if(self.inFlight < concurrency){
      clearWait()
      self.inFlight++
      q.dequeue()
        .then((msg)=>{
          if(msg){
            processAndFinishMsg(msg)
              .catch(err=>{
                console.warn('processing err', err)
                self.inFlight--
                self.pollAndFillSlots()
              })
              .then(()=>{
                console.log('THEN decrement')
                self.inFlight--
                self.pollAndFillSlots()
              })
          } else {
            console.log('queue empty')
            self.inFlight--
            waitThenPoll()
          }
        })
      self.pollAndFillSlots()
    } else {
      waitThenPoll()
    }
  }
  const processAndFinishMsg = async (msg) => {
    if(!msg){
      console.log('no message on queue, returning false')
      return false // 
    }
    console.log('processAndFinishMsg', msg.id)
    try {
      let result = await fn(q, msg)
      console.log('acking processAndFinishMsg', msg.id)
      return await q.ack(msg.id)
    } catch (error) {
      console.log('FAILING processAndFinishMsg', msg.id)
      return await q.fail(msg.id, error)
    }
    console.log('END processAndFinishMsg', msg.id)
  }

  self.pollAndFillSlots()
  
  return {
    stop(){
      clearWait()
      stopped = true
      console.log('processor has been stopped!')
    },
    get inFlight(){
      return self.inFlight
    },
    get pollTimer(){
      return pollTimer
    },
    pollAndFillSlots: self.pollAndFillSlots,
  }
}
module.exports = Processor