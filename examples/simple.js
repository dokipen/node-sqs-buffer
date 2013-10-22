#!/usr/bin/env node
var SqsBuffer = require('../index')
  , buf = SqsBuffer({'queueName': process.env.AWS_QUEUE})

buf.write('hi')
buf.write('there')
process.stdin.pipe(buf)
process.on('SIGINT', function() {
  buf.flush().then(function() {
    process.exit()
  })
  setTimeout(function() {
    console.error('exit without flushing buffer (timeout)')
    process.exit(127)
  }, 5000)
})
