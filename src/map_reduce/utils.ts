import { cpus } from "os";
import cluster, { ClusterSettings } from "cluster";
import { pid } from "process";
import { Queue } from "@tiemma/sonic-core";

export type MasterFn = (workerQueue: Queue, args: any) => any;
export type WorkerFn = (event: MapReduceEvent, args: any) => any;
export type ReduceFn = (resultQueue: Queue, failedQueue: Queue) => any;

export const NUM_CPUS = cpus().length;

export const isMaster = () => {
  return cluster.isMaster || cluster.isPrimary;
};

export const getWorkerName = () => {
  if (isMaster()) {
    return "MASTER";
  }

  return `WORKER-${cluster.worker.id}`;
};

export const Delay = (time = Math.random() * 50) =>
  new Promise((resolve) => setTimeout(resolve, time));
export const getLogger =
  (loggerID: string) =>
  (message: any, date = new Date().toISOString()) => {
    if (process.env["QUIET"]) return;
    // eslint-disable-next-line no-console
    console.log(`${date}: ${loggerID}: ${message}`);
  };
const logger = getLogger(getWorkerName());

export const clusterEvents = {
  MESSAGE: "message",
  DISCONNECT: "disconnect",
};

export const getWorkerID = async (workerQueue: Queue) => {
  while (workerQueue.isEmpty()) {
    await Delay();
  }

  let workerID = workerQueue.dequeue();
  while (!cluster.workers[workerID]) {
    workerID = workerQueue.dequeue();
  }

  return workerID;
};

export interface MapReduceEvent {
  data: any;
  response?: any;
  failed?: boolean;

  //  Internal, not to be directly used
  id?: number;
  SYN?: boolean;
  ACK?: boolean;
  SYN_ACK?: boolean;
}

export const configureWorkers = async (numWorkers: number) => {
  const workerQueue = new Queue();
  const processOrder = new Queue();
  const failedOrder = new Queue();

  for (let i = 0; i < numWorkers; i++) {
    const worker = cluster.fork();

    worker.on(clusterEvents.MESSAGE, (message: MapReduceEvent) => {
      const { id, SYN, SYN_ACK, response, failed } = message;

      if (SYN) {
        // Return signal to worker to start processing
        worker.send({ ACK: true });
      } else if (SYN_ACK || id) {
        workerQueue.enqueue(worker.id);
        logger(`Worker ${worker.id} now available`);
      }

      if (failed) {
        failedOrder.enqueue(message);
      } else if (response) {
        processOrder.enqueue(message);
      }
    });

    worker.on(clusterEvents.DISCONNECT, () => {
      logger(`Gracefully shutting down worker #${worker.id}`);
    });
  }

  logger("Worker queues initializing");
  while (workerQueue.getElements().length !== numWorkers) {
    await Delay();
  }
  logger(`Workers queue populated`);

  return { workerQueue, processOrder, failedOrder };
};

export const initMaster = async (numWorkers: number) => {
  (cluster.setupMaster || cluster.setupPrimary)({
    execArgv: ["-r", "tsconfig-paths/register", "-r", "ts-node/register"],
  } as ClusterSettings);

  logger("Running Map reduce");
  logger(`Process running on pid ${pid}`);

  return configureWorkers(numWorkers);
};

export const initWorkers = async (workerFns: WorkerFn[], args: any) => {
  const logger = getLogger(getWorkerName());

  // Register worker on startup
  process.send({ SYN: true });

  process.on(clusterEvents.MESSAGE, async (event: MapReduceEvent) => {
    // Establish master-node communication with 3-way handshake
    if (event.ACK) {
      logger(`Worker ${cluster.worker.id} now active and processing requests`);

      process.send({ SYN_ACK: true });

      return;
    }

    const res = { id: cluster.worker.id, data: event.data };
    try {
      let response = event.data;

      for (let i = 0; i < workerFns.length; i++) {
        response = await workerFns[i](
          { data: response },
          {
            ...args,
            workerID: cluster.worker.id,
          }
        );
      }
      res["response"] = response;
    } catch (e) {
      logger(`Error occurred: ${e}`);
      res["failed"] = true;
    }

    process.send(res);
  });
};

export const shutdown = async (numWorkers: number) => {
  for (let i = 0; i <= numWorkers; i++) {
    if (cluster.workers[i]) {
      await Delay(100);
      cluster.workers[i].disconnect();
    }
  }

  logger(`Shutting down master`);
};

export const Map = async (workerQueue: Queue, event: MapReduceEvent) => {
  const workerID = await getWorkerID(workerQueue);

  cluster.workers[workerID].send(event);
};
