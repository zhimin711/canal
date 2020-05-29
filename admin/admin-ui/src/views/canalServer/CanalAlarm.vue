<template>
  <div class="app-container">
    <div class="filter-container">
      <el-input v-model="listQuery.name" placeholder="实例名称" style="width: 200px;" class="filter-item" />
      <!-- <el-input v-model="listQuery.ip" placeholder="Server IP" style="width: 200px;" class="filter-item" /> -->
      <el-button class="filter-item" type="primary" icon="el-icon-search" plain @click="fetchData()">查询</el-button>
    </div>
    <el-table v-loading="listLoading" :data="list" border>
      <el-table-column label="信息">
        <template slot-scope="{row}">
          <div>
            <span class="message-title">实例: </span>
            <el-tag type="danger">
              {{ row.name }}
            </el-tag>
          </div>
          <br>
          <div>
            <span class="message-title" style="padding-right: 10px;">类型: </span>
            <el-tag type="warning">
              {{ row.type }}
            </el-tag>
          </div>
          <br>
          <div>
            <span class="message-title" style="padding-right: 16px;">时间: </span>
            <el-tag type="success">
              {{ row.createdTime }}
            </el-tag>
          </div>
        </template>
      </el-table-column>
      <el-table-column label="堆栈">
        <template slot-scope="scope">
          {{ scope.row.message }}
        </template>
      </el-table-column>
    </el-table>
    <pagination v-show="count>0" :total="count" :page.sync="listQuery.page" :limit.sync="listQuery.size" @pagination="fetchData()" />
  </div>
</template>

<script>
import { getCanalAlarms } from '@/api/canalAlarm'
import Pagination from '@/components/Pagination'

export default {
  components: { Pagination },
  filters: {
    statusFilter(status) {
      const statusMap = {
        '1': 'success',
        '0': 'gray',
        '-1': 'danger'
      }
      return statusMap[status]
    },
    statusLabel(status) {
      const statusMap = {
        '1': '启动',
        '0': '停止',
        '-1': '断开'
      }
      return statusMap[status]
    }
  },
  data() {
    return {
      list: null,
      count: 0,
      listLoading: true,
      listQuery: {
        name: '',
        type: '',
        page: 1,
        size: 10
      }
    }
  },
  created() {
    this.fetchData()
  },
  methods: {
    fetchData() {
      this.listLoading = true
      getCanalAlarms(this.listQuery).then(res => {
        this.list = res.data.items
        this.count = res.data.count
      }).finally(() => {
        this.listLoading = false
      })
    }
  }
}
</script>
