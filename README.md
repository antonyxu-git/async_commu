# Async Communication

A mini implementation for ROS lisk communication.

## Task Buckets

- [x] Topic (Single Direct)
- [ ] Service (Double Direct)
- [ ] Action (Goal/Result/Feedback)

Topic: 异步单向，连续单向发送数据
Service: 同步双向，即时等待给出请求的相应
Action: 异步双向，请求与响应间隔太长，需要中途反馈
