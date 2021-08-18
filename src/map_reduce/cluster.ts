import cluster from "cluster";
import {
  Delay,
  initMaster,
  initWorkers,
  isMaster,
  MasterFn,
  NUM_CPUS,
  ReduceFn,
  shutdown,
  WorkerFn,
} from "./utils";

const MapReduce = async (
  masterFn: MasterFn,
  workerFns: WorkerFn[],
  reduceFn: ReduceFn,
  args: any
) => {
  if (isMaster()) {
    const { numWorkers = NUM_CPUS } = args;
    const { workerQueue, processOrder, failedOrder } = await initMaster(
      numWorkers
    );

    await masterFn(workerQueue, args);
    while (workerQueue.getElements().length !== numWorkers) {
      await Delay(1000);
    }

    await shutdown(numWorkers);

    return reduceFn(processOrder, failedOrder);
  } else if (cluster.isWorker) {
    await initWorkers(workerFns, args);
  }
};

export const sonicDistribute = MapReduce;
