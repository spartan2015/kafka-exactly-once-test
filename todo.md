1. produce message - 1000 - to a kafka topic - id from 1 to 1000
2. have at least 2 consumers - same app started twice
3. test in case of failure - coded 
   3.1 each consumer should throw an exception in middle of transaction every other message or using some other algorithm
4. consumer must read the message then write in another topic transactional
5. check all messages were processed
   5.1. output queue should contain exactly 1000 unique messages - original messages generated (same ids)

1 producer spring app
2 consumers - same spring app
3 final consumer for output topic - checks correctness unique messages

Start project: https://github.com/codefreshdemo/example-springboot-kafka