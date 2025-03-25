import { eq, inArray } from 'drizzle-orm/expressions';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { getTestDB } from '@/database/models/__tests__/_util';
import * as Schema from '@/database/schemas';
import { ExportPgDataStructure } from '@/types/export';

import { DataImporterRepos } from '../index';
import agentsData from './fixtures/agents.json';
import userSettingsData from './fixtures/userSettings.json';

const clientDB = await getTestDB();

const userId = 'test-user-id';
let importer: DataImporterRepos;

beforeEach(async () => {
  await clientDB.delete(Schema.users);

  // 创建测试数据
  await clientDB.transaction(async (tx) => {
    await tx.insert(Schema.users).values({ id: userId });
  });

  importer = new DataImporterRepos(clientDB, userId);
});

describe('DataImporter', () => {
  describe('import userSettings', () => {
    const data = userSettingsData as ExportPgDataStructure;
    it('should import userSettings correctly', async () => {
      const result = await importer.importData(data);

      expect(result.success).toBe(true);
      expect(result.results.userSettings).toMatchObject({ added: 1, errors: 0, skips: 0 });

      const res = await clientDB.query.userSettings.findMany({
        where: eq(Schema.userSettings.id, userId),
      });
      expect(res).toHaveLength(1);
      expect(res[0].general).toEqual({ fontSize: 12 });
    });

    it('should merge exist userSettings correctly', async () => {
      await clientDB.transaction(async (tx) => {
        await tx.insert(Schema.userSettings).values({ id: userId, general: { fontSize: 24 } });
        await tx
          .update(Schema.userSettings)
          .set({ general: { fontSize: 24 } })
          .where(eq(Schema.userSettings.id, userId));
      });

      const result = await importer.importData(data);

      expect(result.success).toBe(true);
      expect(result.results.userSettings).toMatchObject({
        updated: 1,
        errors: 0,
        skips: 0,
        added: 0,
      });

      const res = await clientDB.query.userSettings.findMany({
        where: eq(Schema.userSettings.id, userId),
      });
      expect(res).toHaveLength(1);
      expect(res[0].general).toEqual({ fontSize: 12 });
    });
  });

  describe('import agents and sessions', () => {
    it('should import return correct result', async () => {
      const data = agentsData as ExportPgDataStructure;
      const result = await importer.importData(data);

      expect(result.success).toBe(true);
      expect(result.results.agents).toMatchObject({ added: 1, errors: 0, skips: 0 });

      const agentRes = await clientDB.query.agents.findMany({
        where: eq(Schema.agents.userId, userId),
      });
      const sessionRes = await clientDB.query.sessions.findMany({
        where: eq(Schema.sessions.userId, userId),
      });
      const agentsToSessionRes = await clientDB.query.agentsToSessions.findMany({
        where: eq(Schema.agentsToSessions.userId, userId),
      });

      expect(agentRes).toHaveLength(1);
      expect(sessionRes).toHaveLength(1);
      expect(agentsToSessionRes).toHaveLength(1);
      expect(agentsToSessionRes[0]).toMatchObject({
        agentId: agentRes[0].id,
        sessionId: sessionRes[0].id,
      });

      expect(agentRes[0].clientId).toEqual(agentsData.data.agents[0].id);
      expect(sessionRes[0].clientId).toEqual(agentsData.data.sessions[0].id);
    });
  });
});
