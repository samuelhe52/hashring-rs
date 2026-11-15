export type Panel = 'topology' | 'replay' | 'compare' | 'case' | 'experiment' | 'runs' | 'latency' | 'nodes' | 'events' | 'config' | 'observations' | 'files' | 'reference' | 'event'
export type ReferenceView = 'load' | 'balance' | 'acceptance'

export interface DirectoryEntry {
  id: string
  label: string
  panel: Panel | null
  view?: string
  plot?: string
  anchor?: string
  children?: DirectoryEntry[]
}

export interface DirectoryGroup {
  label: string
  items: DirectoryEntry[]
}

export const directory: DirectoryGroup[] = [
  { label: '实验', items: [
    { id: 'overview', label: '运行概览', panel: null },
    { id: 'runs', label: '选择运行', panel: 'runs' },
    { id: 'experiment', label: '运行实验与实时监测', panel: 'experiment' },
  ] },
  { label: '分析', items: [
    { id: 'case', label: '负载档位对比', panel: 'case', children: [
      { id: 'case-stages', label: '档位对比与导出', panel: 'case', view: 'stages' },
      { id: 'case-story', label: '档位数据与采样时刻', panel: 'case', anchor: 'playback-case' },
    ] },
    { id: 'compare', label: '运行对比', panel: 'compare', children: [
      { id: 'compare-bar', label: '阶段指标与差值', panel: 'compare', view: 'latency', plot: 'bar' },
      { id: 'compare-group', label: '分组分位数', panel: 'compare', view: 'latency', plot: 'group' },
      { id: 'compare-curve', label: '负载曲线叠加', panel: 'compare', view: 'latency', plot: 'curve' },
      { id: 'compare-repeat', label: '重复运行统计', panel: 'compare', view: 'latency', anchor: 'compare-repeats' },
      { id: 'compare-other', label: '其他指标', panel: 'compare', view: 'other' },
      { id: 'compare-config', label: '配置差异', panel: 'compare', view: 'config' },
    ] },
    { id: 'replay', label: '历史回放', panel: 'replay', children: [
      { id: 'replay-metrics', label: '指标曲线与播放控制', panel: 'replay', view: 'metrics' },
      { id: 'replay-topology', label: '哈希环动画', panel: 'replay', view: 'topology' },
      { id: 'replay-readings', label: '游标读数', panel: 'replay', anchor: 'playback-current' },
      { id: 'replay-events', label: '事件进程', panel: 'replay', anchor: 'playback-events' },
      { id: 'replay-source', label: '事件字段与来源', panel: 'replay', anchor: 'playback-source' },
    ] },
    { id: 'topology', label: '哈希环与拓扑', panel: 'topology', children: [
      { id: 'topology-range', label: '区间、归属与副本', panel: 'topology', view: 'range' },
      { id: 'topology-nodes', label: '虚拟节点与空间分布', panel: 'topology', view: 'nodes' },
    ] },
  ] },
  { label: '指标与观测', items: [
    { id: 'latency', label: '延迟与速率', panel: 'latency', children: [
      { id: 'latency-curve', label: '负载曲线', panel: 'latency', view: 'curve' },
      { id: 'latency-distribution', label: '延迟分布与图表导出', panel: 'latency', view: 'distribution' },
      { id: 'latency-numbers', label: '数值明细与错误', panel: 'latency', view: 'numbers' },
    ] },
    { id: 'nodes', label: '节点负载', panel: 'nodes', children: [
      { id: 'nodes-keys', label: '键数分布', panel: 'nodes', view: 'owned_keys' },
      { id: 'nodes-operations', label: '操作数分布', panel: 'nodes', view: 'ops_handled' },
      { id: 'nodes-cpu', label: 'CPU 占用', panel: 'nodes', view: 'cpu_percent' },
    ] },
    { id: 'observations', label: '其他观测', panel: 'observations', children: [
      { id: 'observations-throughput', label: '批量吞吐', panel: 'observations', view: 'throughput' },
      { id: 'observations-idle', label: '空闲基线', panel: 'observations', view: 'idle' },
      { id: 'observations-pressure', label: '节点压力', panel: 'observations', view: 'pressure' },
      { id: 'observations-structured', label: '迁移、故障与模式记录', panel: 'observations', view: 'structured' },
      { id: 'observations-other', label: '其他标量记录', panel: 'observations', view: 'other' },
    ] },
  ] },
  { label: '记录与报告', items: [
    { id: 'events', label: '事件时间线', panel: 'events' },
    { id: 'config', label: '实验配置', panel: 'config', children: [
      { id: 'config-parameters', label: '实际参数', panel: 'config', view: 'parameters' },
      { id: 'config-build', label: '构建与环境', panel: 'config', view: 'build' },
    ] },
    { id: 'files', label: '文件与日志', panel: 'files', children: [
      { id: 'files-raw', label: '原始结果', panel: 'files', view: 'raw' },
      { id: 'files-logs', label: '进程日志', panel: 'files', view: 'logs' },
    ] },
    { id: 'reference', label: '实验报告', panel: 'reference', children: [
      { id: 'reference-load', label: '正式负载曲线', panel: 'reference', view: 'load' },
      { id: 'reference-balance', label: '物理节点键数', panel: 'reference', view: 'balance' },
      { id: 'reference-acceptance', label: '验收对照', panel: 'reference', view: 'acceptance' },
    ] },
  ] },
]
