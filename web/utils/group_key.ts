/** 未填写分组名称的条目归入的分组键。 */
export const UNGROUPED_GROUP_KEY = '__ungrouped__';

export const normalizeGroupName = (group?: string) => group?.trim() || '';

export const getGroupKey = (group?: string) => normalizeGroupName(group) || UNGROUPED_GROUP_KEY;
