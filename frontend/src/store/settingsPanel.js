/**
 * 极简跨组件信号：请求打开设置面板
 * SettingsModal 挂载在页面头部，而恢复/续跑相关的提示（例如"图谱写入卡住，
 * 请检查凭据"）出现在 Step3Simulation 内部——二者之间没有共同的父子关系可以
 * 直接传 prop/emit，因此用一个自增计数器作为"打开请求"信号：
 * SettingsModal 侦听该计数器的变化并据此打开自身弹窗。
 */
import { reactive } from 'vue'

const state = reactive({
  openRequestId: 0
})

export function requestOpenSettings() {
  state.openRequestId += 1
}

export default state
