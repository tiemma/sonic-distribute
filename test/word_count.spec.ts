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
  Orchards: 2,
  seemed: 2,
  like: 2,
  a: 2,
  frivolous: 2,
  crop: 2,
  when: 2,
  so: 2,
  many: 2,
  people: 2,
  needed: 2,
  food: 4,
  The: 4,
  golden: 2,
  retriever: 2,
  loved: 2,
  the: 8,
  fireworks: 2,
  each: 2,
  Fourth: 2,
  of: 6,
  July: 2,
  skeleton: 2,
  had: 2,
  skeletons: 2,
  his: 2,
  own: 2,
  in: 2,
  closet: 2,
  They: 2,
  did: 2,
  nothing: 2,
  as: 2,
  raccoon: 2,
  attacked: 2,
  'lady’s': 2,
  bag: 2
};

const masterFn = async (workerQueue: Queue, args: any) => {
  const { dirPath } = args;
  // Work around opendirSync not being in node 10 for regression tests
  const dir = (opendirSync||readdirSync)(dirPath);
  for await (const file of dir) {
    await Map(workerQueue, { data: (file as Dirent).name });
  }
};

const workerFn1 = async (event: MapReduceEvent, args: any) => {
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

const workerFn2 = (event: MapReduceEvent, _: any) => {
  // double everything
  const wordCount: Record<string, number> = event.data
  for(const key of Object.keys(wordCount)) {
    wordCount[key] *= 2
  }

  return wordCount
}

const reduceFn = (queue: Queue) => {
  const wordCounts: Record<string, number> = {};
  while (!queue.isEmpty()) {
    const wordCount = queue.dequeue();
    for (const [word, count] of Object.entries(wordCount.response)) {
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

    const data = await MapReduce(masterFn, [workerFn1, workerFn2], reduceFn, {
      dirPath: `${process.cwd()}/test/fixtures`,
      numWorkers: 1,
    });

    if (isMaster()) {
      expect(data).deep.equal(response);
      process.exit(0);
    }
  });
});
