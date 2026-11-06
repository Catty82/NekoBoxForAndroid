package io.nekohasekai.sagernet.bg

import android.content.Context
import androidx.work.Constraints
import androidx.work.CoroutineWorker
import androidx.work.ExistingPeriodicWorkPolicy.UPDATE
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequest
import androidx.work.PeriodicWorkRequest
import androidx.work.WorkerParameters
import androidx.work.multiprocess.RemoteWorkManager
import io.nekohasekai.sagernet.bg.proto.UrlTest
import io.nekohasekai.sagernet.database.DataStore
import io.nekohasekai.sagernet.database.GroupManager
import io.nekohasekai.sagernet.database.ProfileManager
import io.nekohasekai.sagernet.database.ProxyEntity
import io.nekohasekai.sagernet.database.ProxyGroup
import io.nekohasekai.sagernet.database.SagerDatabase
import io.nekohasekai.sagernet.ktx.Logs
import io.nekohasekai.sagernet.ktx.app
import io.nekohasekai.sagernet.ktx.readableMessage
import io.nekohasekai.sagernet.plugin.PluginManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

object UrlTestUpdater {

    private const val WORK_NAME = "UrlTestUpdater"
    private const val WORK_NAME_ONCE = "UrlTestUpdaterOnce"
    private val minIntervalSec = TimeUnit.HOURS.toSeconds(20).toInt()

    private val networkConstraints = Constraints.Builder()
        .setRequiredNetworkType(NetworkType.CONNECTED)
        .build()

    private suspend fun autoUrlTestGroups(): List<ProxyGroup> {
        return SagerDatabase.groupDao.subscriptions()
            .filter { it.subscription?.autoUrlTest == true }
    }

    private fun isDue(group: ProxyGroup): Boolean {
        val lastAt = group.subscription?.urlTestLastAt ?: 0
        if (lastAt <= 0) return true
        val now = (System.currentTimeMillis() / 1000).toInt()
        return now - lastAt >= minIntervalSec
    }

    suspend fun reconfigureUpdater() {
        val workManager = RemoteWorkManager.getInstance(app)
        workManager.cancelUniqueWork(WORK_NAME)
        workManager.cancelUniqueWork(WORK_NAME_ONCE)

        val groups = autoUrlTestGroups()
        if (groups.isEmpty()) return

        workManager.enqueueUniquePeriodicWork(
            WORK_NAME,
            UPDATE,
            PeriodicWorkRequest.Builder(UpdateTask::class.java, 1, TimeUnit.DAYS)
                .setConstraints(networkConstraints)
                .build()
        )

        if (groups.any(::isDue)) {
            workManager.enqueueUniqueWork(
                WORK_NAME_ONCE,
                ExistingWorkPolicy.REPLACE,
                OneTimeWorkRequest.Builder(UpdateTask::class.java)
                    .setConstraints(networkConstraints)
                    .build()
            )
        }
    }

    class UpdateTask(
        appContext: Context, params: WorkerParameters
    ) : CoroutineWorker(appContext, params) {

        override suspend fun doWork(): Result {
            val groups = autoUrlTestGroups().filter(::isDue)
            if (groups.isEmpty()) return Result.success()

            if (DataStore.runningTest) {
                Logs.d("urlTestAuto: skipped, another test is running")
                return Result.retry()
            }

            DataStore.runningTest = true
            try {
                for (group in groups) {
                    testGroup(group)
                }
            } catch (e: Exception) {
                Logs.w(e)
                return Result.retry()
            } finally {
                DataStore.runningTest = false
            }

            return Result.success()
        }

        private suspend fun testGroup(group: ProxyGroup) {
            val subscription = group.subscription ?: return
            val profilesList = SagerDatabase.proxyDao.getByGroup(group.id)
            val now = (System.currentTimeMillis() / 1000).toInt()

            if (profilesList.isEmpty()) {
                Logs.d("urlTestAuto: no profiles in ${group.displayName()}")
                subscription.urlTestLastAt = now
                subscription.urlTestLastAvailable = 0
                subscription.urlTestLastTotal = 0
                SagerDatabase.groupDao.updateGroup(group)
                GroupManager.postUpdate(group)
                return
            }

            Logs.d("urlTestAuto: testing ${profilesList.size} profiles in ${group.displayName()}")
            val profiles = ConcurrentLinkedQueue(profilesList)
            val available = AtomicInteger(0)
            val updateMutex = Mutex()
            val pendingUpdates = mutableListOf<ProxyEntity>()

            coroutineScope {
                val jobs = List(DataStore.connectionTestConcurrent.coerceAtLeast(1)) {
                    async(Dispatchers.IO) {
                        val urlTest = UrlTest()
                        while (true) {
                            val profile = profiles.poll() ?: break
                            profile.status = 0
                            try {
                                val result = urlTest.doTest(profile)
                                profile.status = 1
                                profile.ping = result
                                profile.error = null
                                available.incrementAndGet()
                            } catch (e: PluginManager.PluginNotFoundException) {
                                profile.status = 2
                                profile.ping = 0
                                profile.error = e.readableMessage
                            } catch (e: Exception) {
                                profile.status = 3
                                profile.ping = 0
                                profile.error = e.readableMessage
                            }
                            updateMutex.withLock {
                                pendingUpdates.add(profile)
                            }
                        }
                    }
                }
                jobs.awaitAll()
            }

            if (pendingUpdates.isNotEmpty()) {
                ProfileManager.updateProfile(pendingUpdates)
            }

            subscription.urlTestLastAt = now
            subscription.urlTestLastAvailable = available.get()
            subscription.urlTestLastTotal = profilesList.size
            SagerDatabase.groupDao.updateGroup(group)
            GroupManager.postUpdate(group)
            Logs.d(
                "urlTestAuto: ${group.displayName()} ${available.get()}/${profilesList.size} available"
            )
        }
    }

}
