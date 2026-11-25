const fastify = require('fastify')({ logger: true })
const fs = require('fs')
const path = require('path')

const MEM_FILE = path.join(__dirname, 'memory.json')

// 初始化记忆文件，不存在就新建空数组
if (!fs.existsSync(MEM_FILE)) {
  fs.writeFileSync(MEM_FILE, JSON.stringify([], null, 2), 'utf8')
}

console.log("==== ENV DEBUG ====")
console.log("process.env.PORT =", process.env.PORT)

const PORT = parseInt(process.env.PORT || 8080, 10)

// 根路由：存活检测
fastify.get('/', async () => {
  return { msg: "Memory Gateway Alive ✅", portGot: PORT }
})

// 获取全部记忆
fastify.get('/memory', async () => {
  const raw = fs.readFileSync(MEM_FILE, 'utf8')
  return { memory: JSON.parse(raw) }
})

// 新增一条记忆
fastify.post('/memory/add', async (req) => {
  const { content } = req.body
  if (!content) return { ok: false, msg: "缺少记忆内容 content" }

  const list = JSON.parse(fs.readFileSync(MEM_FILE, 'utf8'))
  list.push({
    time: new Date().toISOString(),
    content
  })
  fs.writeFileSync(MEM_FILE, JSON.stringify(list, null, 2), 'utf8')
  return { ok: true, msg: "记忆写入成功" }
})

// 清空全部记忆（方便调试）
fastify.post('/memory/clear', async () => {
  fs.writeFileSync(MEM_FILE, JSON.stringify([], null, 2), 'utf8')
  return { ok: true, msg: "记忆已清空" }
})

const start = async () => {
  try {
    await fastify.listen({
      port: PORT,
      host: '0.0.0.0'
    })
    console.log("✅成功监听 0.0.0.0:" + PORT)
  } catch (err) {
    fastify.log.error(err)
    process.exit(1)
  }
}

start()
