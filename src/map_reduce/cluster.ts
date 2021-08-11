import cluster from "cluster";
import {
  initMaster,
  initWorkers,
  isMaster,
  MasterFn,
  ReduceFn,
  shutdown,
  WorkerFn,
} from "./utils";

export const MapReduce = async (
  masterFn: MasterFn,
  workerFn: WorkerFn,
  reduceFn: ReduceFn,
  args: any
) => {
  if (isMaster()) {
    const { workerQueue, processOrder } = await initMaster(args);

    await masterFn(workerQueue, args);

    await shutdown();

    return reduceFn(processOrder);
  } else if (cluster.isWorker) {
    await initWorkers(workerFn, args);
  }
};
