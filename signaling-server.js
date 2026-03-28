/**
 * 实时对讲信令服务器
 * 使用 Node.js + WebSocket 实现
 * 
 * 安装依赖:
 * npm install ws express
 * 
 * 运行:
 * node signaling-server.js
 */

const WebSocket = require('ws')
const express = require('express')
const http = require('http')

const app = express()
const server = http.createServer(app)
const wss = new WebSocket.Server({ server, port: 8080 })

// 存储房间信息
const rooms = new Map() // roomId -> Map<userId, WebSocket>
// 存储用户信息
const users = new Map() // ws -> { id, nickName, avatarUrl, roomId }

console.log('信令服务器启动在端口 8080')

wss.on('connection', (ws) => {
  console.log('新连接')

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message)
      handleMessage(ws, data)
    } catch (err) {
      console.error('解析消息失败:', err)
    }
  })

  ws.on('close', () => {
    handleDisconnect(ws)
  })

  ws.on('error', (error) => {
    console.error('WebSocket 错误:', error)
    handleDisconnect(ws)
  })
})

/**
 * 处理消息
 */
function handleMessage(ws, data) {
  console.log('收到消息:', data.type)

  switch (data.type) {
    case 'join':
      handleJoin(ws, data)
      break

    case 'leave':
      handleLeave(ws, data)
      break

    case 'offer':
    case 'answer':
    case 'ice-candidate':
      // 转发给目标用户
      forwardMessage(ws, data)
      break

    case 'speak-start':
    case 'speak-stop':
      // 广播给房间内其他用户
      broadcastToRoom(ws, data)
      break

    case 'audio-data':
      // 转发音频数据
      broadcastToRoom(ws, data)
      break

    default:
      console.log('未知消息类型:', data.type)
  }
}

/**
 * 处理加入房间
 */
function handleJoin(ws, data) {
  const { roomId, user } = data

  if (!rooms.has(roomId)) {
    rooms.set(roomId, new Map())
  }

  const room = rooms.get(roomId)
  room.set(user.id, ws)
  users.set(ws, {
    id: user.id,
    nickName: user.nickName,
    avatarUrl: user.avatarUrl,
    roomId: roomId
  })

  console.log(`用户 ${user.nickName} 加入房间 ${roomId}`)

  // 通知房间内其他用户
  room.forEach((clientWs, clientId) => {
    if (clientId !== user.id && clientWs.readyState === WebSocket.OPEN) {
      clientWs.send(JSON.stringify({
        type: 'user-joined',
        user: user
      }))
    }
  })

  // 向新用户发送当前房间成员列表
  room.forEach((clientWs, clientId) => {
    const userInfo = users.get(clientWs)
    if (userInfo && clientId !== user.id) {
      ws.send(JSON.stringify({
        type: 'user-joined',
        user: {
          id: userInfo.id,
          nickName: userInfo.nickName,
          avatarUrl: userInfo.avatarUrl
        }
      }))
    }
  })
}

/**
 * 处理离开房间
 */
function handleLeave(ws, data) {
  const userInfo = users.get(ws)
  if (userInfo) {
    const { roomId, id } = userInfo
    const room = rooms.get(roomId)
    
    if (room) {
      room.delete(id)
      
      // 通知房间内其他用户
      room.forEach((clientWs, clientId) => {
        if (clientWs.readyState === WebSocket.OPEN) {
          clientWs.send(JSON.stringify({
            type: 'user-left',
            user: {
              id: id,
              nickName: userInfo.nickName,
              avatarUrl: userInfo.avatarUrl
            }
          }))
        }
      })

      // 如果房间为空，删除房间
      if (room.size === 0) {
        rooms.delete(roomId)
      }
    }

    users.delete(ws)
    console.log(`用户 ${userInfo.nickName} 离开房间 ${roomId}`)
  }
}

/**
 * 处理断开连接
 */
function handleDisconnect(ws) {
  handleLeave(ws, {})
}

/**
 * 转发消息给目标用户
 */
function forwardMessage(fromWs, data) {
  const fromUser = users.get(fromWs)
  if (!fromUser) return

  const room = rooms.get(fromUser.roomId)
  if (!room) return

  const targetWs = room.get(data.target)
  if (targetWs && targetWs.readyState === WebSocket.OPEN) {
    targetWs.send(JSON.stringify({
      ...data,
      from: fromUser.id
    }))
  }
}

/**
 * 广播消息给房间内其他用户
 */
function broadcastToRoom(fromWs, data) {
  const fromUser = users.get(fromWs)
  if (!fromUser) return

  const room = rooms.get(fromUser.roomId)
  if (!room) return

  room.forEach((clientWs, clientId) => {
    if (clientId !== fromUser.id && clientWs.readyState === WebSocket.OPEN) {
      clientWs.send(JSON.stringify({
        ...data,
        userId: fromUser.id
      }))
    }
  })
}

// 健康检查接口
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    rooms: rooms.size,
    users: users.size
  })
})

// 启动服务器
server.listen(8080, () => {
  console.log('服务器运行在 http://localhost:8080')
})
