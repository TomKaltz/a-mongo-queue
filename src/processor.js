const EventEmitter = require('events').EventEmitter

const Processor = function(q, fn, opts = {}){
  const debug = require('debug')(`processor:${opts.concurrency||5}:${opts.pollInterval||'nopoll'}`)
  debug('*****Processor Constructor*******')
  if(!q || q.constructor.name !== 'Queue') throw new Error('Processor: you must pass in a valid Queue object')
  if(!fn) throw new Error('Processor: you must pass in a function')

  const self = new EventEmitter()
  
  let pollInterval = opts.pollInterval || null
  let concurrency = opts.concurrency || 5
  self.inFlight = 0
  let pendingInFlight = 0
  let pollTimer = null
  let stopped = false
  let emptyCount = 0

  
  const clearWait = () => {
    if(pollTimer){
      debug('clearing timeout')
      clearTimeout(pollTimer)
      pollTimer = null
    }
  }
  
  const waitThenPoll = () => {
    clearWait()
    if(pollInterval){
      debug('waiting', pollInterval, self.inFlight)
      pollTimer = setTimeout(self.pollAndFillSlots, pollInterval)
    }else{
      debug('pollInterval not set...you must call pollAndFillSlots manually to nudge the processor')
    }
  }

  let count = 0

  self.pollAndFillSlots = () => {
    count++
    if (self.inFlight === 0 && pendingInFlight === 0 && count === 4){
      debugger
    }
    debug('pollAndFillSlots called - inFlight', self.inFlight,'pending', pendingInFlight,'count', count)
    if((pendingInFlight + self.inFlight) < concurrency){
      clearWait()
      pendingInFlight++
      q.dequeue()
        .then((msg)=>{
          pendingInFlight--
          if(msg){
            emptyCount = 0
            self.inFlight++
            processAndFinishMsg(msg)
              .then(()=>{
                debug('THEN decrement')
                self.inFlight--
                self.pollAndFillSlots()
              })
              .catch(err=>{
                console.warn('processing err', err)
                self.inFlight--
                self.pollAndFillSlots()
              })
            // self.pollAndFillSlots()
          } else {
            emptyCount++
            debug('queue empty',emptyCount)
            waitThenPoll()
          }
        })
        .catch((err)=>{
          debug('dequeue catch', err)
          pendingInFlight--
          waitThenPoll()
          // self.pollAndFillSlots()
        })
    } else if(pendingInFlight > 0) {
      debug('pendingInFlight > 0....calling again')
      self.pollAndFillSlots()
    }
    else {
      waitThenPoll()
    }
  }

  const processAndFinishMsg = async (msg) => {
    if(!msg){
      debug('no message on queue, returning false')
      return false // 
    }
    debug('processAndFinishMsg', msg.id)
    try {
      let result = await fn(q, msg)
      debug('acking processAndFinishMsg', msg.id)
      return await q.ack(msg.id)
    } catch (error) {
      debug('FAILING processAndFinishMsg', msg.id)
      return await q.fail(msg.id, error)
    }
    debug('END processAndFinishMsg', msg.id)
  }

  self.pollAndFillSlots()
  
  return {
    stop(force){
      // TODO: Need to implement graceful shutdown
      clearWait()
      stopped = true
      debug('processor has been stopped!')
    },
    get inFlight(){
      return self.inFlight
    },
    get pendingInFlight(){
      return pendingInFlight
    },
    get pollTimer(){
      return pollTimer
    },
    pollAndFillSlots: self.pollAndFillSlots,
  }
}
module.exports = Processor