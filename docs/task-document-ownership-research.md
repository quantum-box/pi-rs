# Task document ownership investigation

Reference: upstream pi `04fd0897e4df737cd8422bc99085d6953da804ec`,
`packages/durable/docs/spec.md` §§3.3, 5.5, 9.1 and
`packages/durable/src/session/transaction.ts`.

Upstream validates task-document acquisition against the latest candidate task,
rejects a terminal candidate, and retires all of its documents in the same
commit as terminal settlement, including documents created in that transaction.
The completing hold commits other writes but keeps these documents until owned
work drains. Its task-document watch delivers the terminal retirement then ends;
its retirement frame preserves the last delivered value, not the final draft.

At base `c563adf49c227d5aea98ac37dd5520d4a3673349`, pi-rs had no enforced
task document address or ownership. `Address.scope` is an unrestricted host
convention, and `TaskDocuments` can write arbitrary addresses. A scan keyed only
by a new scope string would not prevent unfenced writes through the existing
`Documents` and `Transaction` APIs. A correct implementation must enforce
ownership across every write path and fence document creation against terminal
settlement. It must also reconcile staged document writes with retirement in
one transaction, cover task-store terminal transitions including abort and
failure, and preserve the completing graph fence. The original pull watch emitted
a deletion without the previous value and did not end with the task. The subsequent
explicit ownership implementation is described in the current parity checkpoint;
its pull watch and ownership API remain a subset of upstream.
