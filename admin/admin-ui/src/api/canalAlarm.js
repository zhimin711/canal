import request from '@/utils/request'

export function getCanalAlarms(params) {
  return request({
    url: '/canal/alarms',
    method: 'get',
    params: params
  })
}
