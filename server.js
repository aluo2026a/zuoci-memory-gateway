import Fastify from 'fastify';

const fastify = Fastify({ logger: true });
const PORT = process.env.PORT || 3000;

fastify.post('/v1/chat/completions', async (req, reply) => {
  fastify.log.info("收到请求！", req.body.messages?.length);
  return {
    choices: [
      {
        message: {
          role: "assistant",
          content: "网关测试连通成功✅"
        }
      }
    ]
  };
});

fastify.listen({ port: PORT, host: '0.0.0.0' }, (err) => {
  if (err) throw err;
  fastify.log.info(`服务启动成功，端口 ${PORT}`);
});
