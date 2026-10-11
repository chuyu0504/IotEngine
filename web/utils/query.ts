export function createQueryKeys<T extends string>(module: T) {
    return {
        all: [module] as const,
        lists: () => [module, 'list'] as const,
        detail: (id: number | string) => [module, 'detail', id] as const,
        options: () => [module, 'options'] as const,
    };
}
