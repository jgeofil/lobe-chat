import { and, eq, inArray } from 'drizzle-orm/expressions';

import * as SCHEMAS from '@/database/schemas';
import { LobeChatDatabase } from '@/database/type';
import { ImportResult } from '@/services/import/_deprecated';
import { ExportDatabaseData } from '@/types/export';
import { ImporterEntryData } from '@/types/importer';

import { DeprecatedDataImporterRepos } from './deprecated';

interface ImportProcessorParams {
  idMappings: Record<string, Record<string, string>>;
  mode: ImportMode;
  tableConfig: TableImportConfig;
}

type ImportProcessor = (
  data: any[],
  trx: any,
  params: ImportProcessorParams,
) => Promise<ImportResult>;

interface TableImportConfig {
  // 特殊处理函数（可选）
  dependencies?: string[];
  // 依赖的表（可选）
  foreignKeys?: Record<string, string>;
  // 表名
  processor?: ImportProcessor;
  tableName: string; // 字段到表的映射（可选）
}

// 导入模式
export enum ImportMode {
  OVERRIDE = 'override',
  SKIP = 'skip',
}

export class DataImporterRepos {
  private userId: string;
  private db: LobeChatDatabase;
  private foreignKeyRelations: Record<string, Record<string, string>>;
  private deprecatedDataImporterRepos: DeprecatedDataImporterRepos;

  constructor(db: LobeChatDatabase, userId: string) {
    this.userId = userId;
    this.db = db;
    // 在构造函数中初始化外键关系缓存
    this.foreignKeyRelations = this.extractForeignKeyRelations();
    this.deprecatedDataImporterRepos = new DeprecatedDataImporterRepos(db, userId);
  }

  importData = async (data: ImporterEntryData) => {
    return this.deprecatedDataImporterRepos.importData(data);
  };

  /**
   * 处理标准表数据
   */
  private processTable =
    (tableName: string): ImportProcessor =>
    async (data, trx, { mode, idMappings, tableConfig }) => {
      const result: ImportResult = { added: 0, errors: 0, skips: 0, updated: 0 };

      // @ts-expect-error
      if (!SCHEMAS[tableName]) {
        console.warn(`Schema not found for table: ${tableName}`);
        result.errors = 1;
        return result;
      }

      // @ts-expect-error
      const tableSchema = SCHEMAS[tableName];

      // 1. 检查已存在的记录（基于 clientId 和 userId）
      let existingRecords = [];
      try {
        // 确保 tableSchema.clientId 和 tableSchema.userId 存在
        if (tableSchema.clientId && tableSchema.userId) {
          existingRecords = await trx.query[tableName].findMany({
            where: and(
              eq(tableSchema.userId, this.userId),
              inArray(
                tableSchema.clientId,
                data.map((item) => item.clientId || item.id).filter(Boolean),
              ),
            ),
          });
        }
      } catch (error) {
        console.error(`Error fetching existing records for ${tableName}:`, error);
        result.errors = 1;
        return result;
      }

      // 获取已存在记录的 clientId 映射
      const existingClientIdMap = new Map(
        existingRecords.map((record) => [record.clientId || record.id, record]),
      );

      // 2. 根据模式处理数据
      let recordsToInsert = [];
      let recordsToUpdate = [];

      for (const item of data) {
        const clientId = item.clientId || item.id;
        const existing = existingClientIdMap.get(clientId);

        // 准备记录数据
        const recordData = this.prepareRecordData(item, idMappings, tableConfig.foreignKeys);

        if (existing) {
          // 记录已存在
          if (mode === ImportMode.OVERRIDE) {
            // 覆盖模式：更新记录
            recordsToUpdate.push({ data: recordData, id: existing.id });
          } else {
            // 跳过模式：记录跳过
            result.skips++;
          }

          // 无论是否更新，都需要添加到 ID 映射
          idMappings[tableName][clientId] = existing.id;
        } else {
          // 记录不存在：插入新记录
          recordsToInsert.push(recordData);
        }
      }

      // 3. 插入新记录
      if (recordsToInsert.length > 0) {
        try {
          // 修复：确保返回的字段在表结构中存在
          const insertResult = await trx.insert(tableSchema).values(recordsToInsert);

          // 如果需要获取插入的ID，使用单独的查询
          if (insertResult.rowCount > 0) {
            // 查询刚插入的记录
            const insertedIds = await trx.query[tableName].findMany({
              columns: {
                clientId: true,
                id: true,
              },
              where: and(
                eq(tableSchema.userId, this.userId),
                inArray(
                  tableSchema.clientId,
                  recordsToInsert.map((r) => r.clientId).filter(Boolean),
                ),
              ),
            });

            // 更新 ID 映射表
            insertedIds.forEach((record) => {
              if (record.clientId) {
                idMappings[tableName][record.clientId] = record.id;
              }
            });
          }

          result.added = insertResult.rowCount || 0;
        } catch (error) {
          console.error(`Error inserting records for ${tableName}:`, error);
          result.errors += recordsToInsert.length;
        }
      }

      // 4. 更新现有记录（如果是覆盖模式）
      if (recordsToUpdate.length > 0) {
        let updatedCount = 0;
        for (const record of recordsToUpdate) {
          try {
            const updateResult = await trx
              .update(tableSchema)
              .set({ ...record.data, updatedAt: new Date() })
              .where(eq(tableSchema.id, record.id));

            if (updateResult.rowCount > 0) {
              updatedCount++;
            }
          } catch (error) {
            console.error(`Error updating record for ${tableName}:`, error);
            result.errors++;
          }
        }

        result.updated = updatedCount;
      }

      return result;
    };

  /**
   * 准备记录数据 - 使用缓存的外键信息处理 ID 和外键引用
   */
  private prepareRecordData(
    tableName: string,
    item: any,
    idMappings: Record<string, Record<string, string>>,
    options?: { deleteId: boolean },
  ): any {
    const { deleteId } = options || { deleteId: true };

    // 创建新记录对象，保留原始 ID 到 clientId
    const newItem: any = {
      ...item,
      clientId: item.clientId || item.id,
      userId: this.userId,
    };

    // 处理日期字段
    if (newItem.createdAt) newItem.createdAt = new Date(newItem.createdAt);
    if (newItem.updatedAt) newItem.updatedAt = new Date(newItem.updatedAt);
    if (newItem.accessedAt) newItem.accessedAt = new Date(newItem.accessedAt);

    // 处理外键引用 - 使用缓存的外键信息
    const tableRelations = this.foreignKeyRelations[tableName];

    console.log(tableRelations);
    if (tableRelations) {
      Object.entries(newItem).forEach(([key, value]) => {
        // 如果字段是外键且有值
        if (tableRelations[key] && value) {
          const refTableName = tableRelations[key];
          // 检查引用表的ID映射
          if (idMappings[refTableName] && idMappings[refTableName][value as string]) {
            newItem[key] = idMappings[refTableName][value as string];
          }
        }
      });
    }

    // 删除原始 id 字段，让数据库生成新 id
    if (deleteId) {
      delete newItem.id;
    }

    return newItem;
  }

  /**
   * 处理 messages 表的特殊情况
   */
  private processMessages = async (
    messages: any[],
    trx: any,
    { idMappings, mode, tableConfig }: ImportProcessorParams,
  ): Promise<ImportResult> => {
    // 1. 先处理所有消息，暂时将 parentId 设为 null
    const messagesWithoutParent = messages.map((msg) => {
      const newMsg = { ...msg };
      // 保存原始 parentId 到临时字段
      if (newMsg.parentId) {
        newMsg._original_parentId = newMsg.parentId;
        newMsg.parentId = null;
      }
      return newMsg;
    });

    // 2. 插入所有消息
    const result = await this.processTable('messages')(messagesWithoutParent, trx, {
      idMappings,
      mode,
      tableConfig,
    });

    // 3. 更新 parentId 关系
    const parentUpdates = messages
      .filter((msg) => msg.parentId)
      .map((msg) => {
        const clientId = msg.clientId || msg.id;
        const parentClientId = msg.parentId;

        const newMessageId = idMappings.messages[clientId];
        const newParentId = idMappings.messages[parentClientId];

        if (newMessageId && newParentId) {
          return {
            messageId: newMessageId,
            parentId: newParentId,
          };
        }
        return null;
      })
      .filter(Boolean);

    // 批量更新 parentId
    if (parentUpdates.length > 0) {
      console.log(`Updating ${parentUpdates.length} parent-child relationships for messages`);

      // 逐个更新而不是使用CASE语句，避免语法错误
      for (const update of parentUpdates) {
        try {
          await trx
            .update(SCHEMAS.messages)
            .set({ parentId: update.parentId })
            .where(eq(SCHEMAS.messages.id, update.messageId));
        } catch (error) {
          console.error(`Error updating message parent relationship:`, error);
        }
      }
    }

    return result;
  };

  /**
   * 处理用户设置表
   */
  private processUserSettings = async (
    data: any[],
    trx: any,
    { idMappings }: ImportProcessorParams,
  ): Promise<ImportResult> => {
    const result: ImportResult = { added: 0, errors: 0, skips: 0, updated: 0 };

    // 检查是否已存在用户设置
    const existingSettings = await trx.query.userSettings.findFirst({
      where: eq(SCHEMAS.userSettings.id, this.userId),
    });

    // 如果有数据要导入
    if (data.length > 0) {
      const settingsData = this.prepareRecordData('userSettings', data[0], idMappings);
      settingsData.id = this.userId; // 使用当前用户ID

      if (existingSettings) {
        // 已存在设置，更新设置
        await trx
          .update(SCHEMAS.userSettings)
          .set(settingsData)
          .where(eq(SCHEMAS.userSettings.id, this.userId));
        result.updated = 1;
      } else {
        // 创建新设置
        await trx.insert(SCHEMAS.userSettings).values(settingsData);
        result.added = 1;
      }

      // 更新ID映射
      if (data[0].id) {
        idMappings.userSettings = idMappings.userSettings || {};
        idMappings.userSettings[data[0].id] = this.userId;
      }
    }

    return result;
  };

  /**
   * 处理用户设置表
   */
  private processWithSameIdTable =
    (tableName: string): ImportProcessor =>
    async (records, trx, { idMappings }): Promise<ImportResult> => {
      const result: ImportResult = { added: 0, errors: 0, skips: 0, updated: 0 };

      // 如果有数据要导入
      if (records.length > 0) {
        const data = records.map((item) =>
          this.prepareRecordData(tableName, item, idMappings, { deleteId: false }),
        );

        await trx
          .insert(SCHEMAS[tableName])
          .values(data.map((item) => ({ ...item, userId: this.userId })))
          .onConflictDoNothing();

        result.updated = 1;
      }

      return result;
    };

  /**
   * 处理用户设置表
   */
  private processWithSlugTable =
    (tableName: string): ImportProcessor =>
    async (records, trx, { idMappings }): Promise<ImportResult> => {
      const result: ImportResult = { added: 0, errors: 0, skips: 0, updated: 0 };

      // 如果有数据要导入
      if (records.length > 0) {
        const data = records.map((item) => this.prepareRecordData(tableName, item, idMappings));

        await trx
          .insert(SCHEMAS[tableName])
          .values(data.map((item) => ({ ...item, userId: this.userId })))
          .onConflictDoNothing();

        result.updated = 1;
      }

      return result;
    };

  /**
   * 从 schema 中提取外键关系信息
   */
  private extractForeignKeyRelations(): Record<string, Record<string, string>> {
    const relations: Record<string, Record<string, string>> = {};

    // 遍历所有表的 schema
    Object.entries(SCHEMAS).forEach(([tableName, tableSchema]) => {
      if (!relations[tableName]) relations[tableName] = {};

      // 检查表定义中是否有外键定义
      if (tableSchema._def?.foreignKeys) {
        tableSchema._def.foreignKeys.forEach((fk) => {
          // 获取外键列名和引用表名
          const columnName = fk.columns[0].name;
          const refTableName = fk.foreignColumns[0].table;

          // 存储映射关系
          relations[tableName][columnName] = refTableName;
        });
      }
    });

    // 添加特殊情况处理（如果有的话）
    this.addSpecialRelations(relations);

    return relations;
  }

  /**
   * 添加特殊的外键关系（schema中可能没有明确定义的）
   */
  private addSpecialRelations(relations: Record<string, Record<string, string>>): void {
    // 处理自引用关系
    if (!relations.messages) relations.messages = {};
    relations.messages.parentId = 'messages';
  }

  // 定义表处理顺序（基于依赖关系）
  // 整合所有导入信息的配置
  tableImportOrders: TableImportConfig[] = [
    {
      dependencies: ['users'],
      foreignKeys: { id: 'users' },
      processor: this.processUserSettings,
      tableName: 'userSettings',
    },
    {
      dependencies: ['users'],
      foreignKeys: { userId: 'users' },
      tableName: 'userInstalledPlugins',
    },
    {
      dependencies: ['users'],
      foreignKeys: { userId: 'users' },
      processor: this.processWithSameIdTable('aiProviders'),
      tableName: 'aiProviders',
    },
    {
      dependencies: ['users', 'aiProviders'],
      foreignKeys: { providerId: 'aiProviders', userId: 'users' },
      processor: this.processWithSameIdTable('aiModels'),
      tableName: 'aiModels',
    },
    {
      dependencies: ['users'],
      foreignKeys: { userId: 'users' },
      tableName: 'sessionGroups',
    },
    {
      dependencies: ['users', 'sessionGroups'],
      foreignKeys: { groupId: 'sessionGroups', userId: 'users' },
      processor: this.processWithSlugTable('sessions'),
      tableName: 'sessions',
    },
    {
      dependencies: ['users'],
      foreignKeys: { userId: 'users' },
      processor: this.processWithSlugTable('agents'),
      tableName: 'agents',
    },
    {
      dependencies: ['users', 'agents', 'sessions'],
      foreignKeys: { agentId: 'agents', sessionId: 'sessions', userId: 'users' },
      tableName: 'agentsToSessions',
    },
    {
      dependencies: ['users', 'sessions'],
      foreignKeys: { sessionId: 'sessions', userId: 'users' },
      tableName: 'topics',
    },
    {
      dependencies: ['users', 'sessions', 'topics', 'agents'],
      foreignKeys: {
        agentId: 'agents',
        parentId: 'messages',
        // 自引用
        quotaId: 'messages',

        sessionId: 'sessions',

        topicId: 'topics',
        userId: 'users', // 自引用
      },
      processor: this.processMessages,
      tableName: 'messages',
    },
    {
      dependencies: ['users', 'messages'],
      foreignKeys: { id: 'messages', userId: 'users' },
      tableName: 'messagePlugins',
    },
    {
      dependencies: ['users', 'messages'],
      foreignKeys: { id: 'messages', userId: 'users' },
      tableName: 'messageTranslates',
    },
    {
      dependencies: ['users', 'messages', 'files'],
      foreignKeys: { fileId: 'files', id: 'messages', userId: 'users' },
      tableName: 'messageTts',
    },
    {
      dependencies: ['users', 'topics', 'messages'],
      foreignKeys: {
        parentThreadId: 'threads',
        sourceMessageId: 'messages',
        topicId: 'topics',
        userId: 'users', // 自引用
      },
      tableName: 'threads',
    },
  ];

  /**
   * 导入 pg 导出的数据
   */
  async importPgData(
    dbData: ExportDatabaseData,
    mode: ImportMode = ImportMode.SKIP,
  ): Promise<Record<string, ImportResult>> {
    // 结果统计对象
    const results: Record<string, ImportResult> = {};

    // 使用单一事务包装整个导入过程
    await this.db.transaction(async (trx) => {
      console.log(`Starting data import transaction (mode: ${mode})`);

      const pgData = dbData.data;
      // 初始化 ID 映射表
      const idMappings: Record<string, Record<string, string>> = {};
      Object.entries(pgData).forEach(([table, records]) => {
        if (records.length > 0) {
          idMappings[table] = {};
        }
      });

      // TODO: 特殊处理：先不导入 user 配置数据，后面再看
      delete pgData.users;

      // 按顺序处理每个表
      for (const tableConfig of this.tableImportOrders) {
        const { tableName, processor } = tableConfig;
        const tableData = pgData[tableName];

        if (!tableData || tableData.length === 0) continue;

        console.log(`Processing table: ${tableName} (${tableData.length} records),idMappings:`, {
          ...idMappings,
        });

        try {
          if (processor) {
            results[tableName] = await processor(tableData, trx, {
              idMappings,
              mode,
              tableConfig,
            });
          } else {
            results[tableName] = await this.processTable(tableName)(tableData, trx, {
              idMappings,
              mode,
              tableConfig,
            });
          }

          console.log(
            `Completed table ${tableName}: added=${results[tableName].added}, skips=${results[tableName].skips}, updated=${results[tableName].updated || 0}`,
          );
        } catch (error) {
          console.error(`Error processing table ${tableName}:`, error);
          results[tableName] = { added: 0, errors: 1, skips: 0 };
          // 不抛出错误，继续处理其他表
        }
      }

      // 处理自引用关系
      await this.processSelfReferences(trx, idMappings);

      console.log('Data import transaction completed successfully');
    });

    return results;
  }
}
