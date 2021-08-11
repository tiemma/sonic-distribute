import { opendirSync, readdirSync, readFileSync, Dirent } from "fs";
import { expect, use } from "chai";
import deepEqualInAnyOrder from "deep-equal-in-any-order";
import { Queue } from "@tiemma/sonic-core";
import {
  getWorkerName,
  isMaster,
  Map,
  MapReduce,
  MapReduceEvent,
} from "../src";

use(deepEqualInAnyOrder);

const response = {
  Orchards: 1,
  seemed: 1,
  like: 1,
  a: 1,
  frivolous: 1,
  crop: 1,
  when: 1,
  so: 1,
  many: 1,
  people: 1,
  needed: 1,
  food: 2,
  The: 2,
  golden: 1,
  retriever: 1,
  loved: 1,
  the: 4,
  fireworks: 1,
  each: 1,
  Fourth: 1,
  of: 3,
  July: 1,
  skeleton: 1,
  had: 1,
  skeletons: 1,
  his: 1,
  own: 1,
  in: 1,
  closet: 1,
  They: 1,
  did: 1,
  nothing: 1,
  as: 1,
  raccoon: 1,
  attacked: 1,
  "lady’s": 1,
  bag: 1,
};

const masterFn = async (workerQueue: Queue, args: any) => {
  const { dirPath } = args;
  // Work around opendirSync not being in node 10 for regression tests
  const dir = (opendirSync||readdirSync)(dirPath);
  for await (const file of dir) {
    await Map(workerQueue, { data: (file as Dirent).name });
  }
};

const workerFn = async (event: MapReduceEvent, args: any) => {
  const { dirPath } = args;
  const fileName = event.data;
  const file = await readFileSync(`${dirPath}/${fileName}`, {
    encoding: "utf-8",
  });

  const wordCount: Record<string, number> = {};
  for (const word of file.split(" ")) {
    if (!wordCount[word]) {
      wordCount[word] = 0;
    }
    wordCount[word] += 1;
  }

  return wordCount;
};

const reduceFn = (queue: Queue) => {
  const wordCounts: Record<string, number> = {};
  while (!queue.isEmpty()) {
    const wordCount = queue.dequeue();
    for (const [word, count] of Object.entries(wordCount.data)) {
      if (!wordCounts[word]) {
        wordCounts[word] = 0;
      }
      wordCounts[word] += count as number;
    }
  }

  return wordCounts;
};

describe(`Map reduce - ${getWorkerName()}`, () => {
  it("map reduce works as expected", async () => {
    // defer printing logs
    process.env["QUIET"] = "true";

    const data = await MapReduce(masterFn, workerFn, reduceFn, {
      dirPath: `${process.cwd()}/test/fixtures`,
      numWorkers: 1,
    });

    if (isMaster()) {
      expect(data).deep.equal(response);
      process.exit(0);
    }
  });
});
