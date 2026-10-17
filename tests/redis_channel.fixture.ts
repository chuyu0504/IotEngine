// 共享频道必须使用实际 Redis URL 的逻辑数据库；持久键及专属 UUID 频道不经过此入口。
export function sharedChannel(redisUrl: string, name: string) {
    const database = Number(new URL(redisUrl).pathname.slice(1) || 0);
    return `iot:db:${database}:${name}`;
}
