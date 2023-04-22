import IconButton from "@material-ui/core/IconButton";
import TableCell from "@material-ui/core/TableCell";
import TableRow from "@material-ui/core/TableRow";
import Tooltip from "@material-ui/core/Tooltip";
import FileCopyOutlinedIcon from "@material-ui/icons/FileCopyOutlined";
import React from "react";
import {connect, ConnectedProps} from "react-redux";
import {useHistory} from "react-router-dom";
import {taskRowsPerPageChange} from "../actions/settingsActions";
import {
  batchCancelActiveTasksAsync,
  cancelActiveTaskAsync,
  cancelAllActiveTasksAsync,
  listActiveTasksAsync,
} from "../actions/tasksActions";
import {taskDetailsPath} from "../paths";
import {AppState} from "../store";
import {TableColumn} from "../types/table";
import {prettifyPayload} from "../utils";
import SyntaxHighlighter from "./SyntaxHighlighter";
import TasksTable, {RowProps, useRowStyles} from "./TasksTable";

function mapStateToProps(state: AppState) {
  return {
    loading: state.tasks.activeTasks.loading,
    error: state.tasks.activeTasks.error,
    tasks: state.tasks.activeTasks.data,
    batchActionPending: state.tasks.activeTasks.batchActionPending,
    allActionPending: state.tasks.activeTasks.allActionPending,
    pollInterval: state.settings.pollInterval,
    pageSize: state.settings.taskRowsPerPage,
  };
}

const mapDispatchToProps = {
  listTasks: listActiveTasksAsync,
  cancelTask: cancelActiveTaskAsync,
  batchCancelTasks: batchCancelActiveTasksAsync,
  cancelAllTasks: cancelAllActiveTasksAsync,
  taskRowsPerPageChange,
};

const columns: TableColumn[] = [
  {key: "type", label: "Type", align: "left"},
  {key: "status", label: "Status", align: "left"},
  {key: "payload", label: "Payload", align: "left"},
  {key: "error_count", label: "Error count", align: "left"},
  {key: "error_message", label: "Error message", align: "left"},
  {key: "priority", label: "Priority", align: "left"},
  {key: "created_at", label: "Created At", align: "left"},
];

const connector = connect(mapStateToProps, mapDispatchToProps);

type ReduxProps = ConnectedProps<typeof connector>;

interface Props {
  queue: string; // name of the queue
  totalTaskCount: number; // total number of active tasks
  queryRequest: string;
}

function Row(props: RowProps) {
  const {task} = props;
  const classes = useRowStyles();
  const history = useHistory();
  return (
    <TableRow
      key={task.id}
      className={classes.root}
      selected={props.isSelected}
      onClick={() => history.push(taskDetailsPath(task.queue, task.id))}
    >
      <TableCell component="th" scope="row" className={classes.idCell}>
        <div className={classes.IdGroup}>
          {task.id}
          <Tooltip title="Copy full ID to clipboard">
            <IconButton
              onClick={(e) => {
                e.stopPropagation();
                navigator.clipboard.writeText(task.id);
              }}
              size="small"
              className={classes.copyButton}
            >
              <FileCopyOutlinedIcon fontSize="small"/>
            </IconButton>
          </Tooltip>
        </div>
      </TableCell>
      <TableCell>{task.type}</TableCell>
      <TableCell>{task.status}</TableCell>
      <TableCell>
        <SyntaxHighlighter
          language="json"
          customStyle={{margin: 0, maxWidth: 400}}
        >
          {prettifyPayload(task.payload)}
        </SyntaxHighlighter>
      </TableCell>
      <TableCell>{task.error_count}</TableCell>
      <TableCell>{task.error_message}</TableCell>
      <TableCell>{task.priority}</TableCell>
      <TableCell>{task.created_at}</TableCell>
    </TableRow>
  );
}

function ActiveTasksTable(props: Props & ReduxProps) {
  return (
    <TasksTable
      taskState="active"
      columns={columns}
      renderRow={(rowProps: RowProps) => <Row {...rowProps} />}
      {...props}
    />
  );
}

export default connector(ActiveTasksTable);
