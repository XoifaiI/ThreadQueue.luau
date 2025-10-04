--[=[
	ThreadQueue: Manage asynchronous task execution with rate limiting
	
	Example usage:
		local Queue = ThreadQueue.New(0.1, 50, false)
		local Success, Result = Queue:SubmitAsync(function()
			return "Task completed"
		end)
--]=]

--!strict

local ThreadQueue = {}
ThreadQueue.__index = ThreadQueue

type QueueEntry = {
	Thread: thread,
	Callback: () -> any,
}

type ThreadQueueData = {
	Queue: {QueueEntry},
	QueueRunning: boolean,
	TimeBetween: number,
	MaxQueueLength: number,
	EnableConcurrency: boolean,
}

export type ThreadQueue = typeof(setmetatable({} :: ThreadQueueData, ThreadQueue))

-- Honestly don't remember what this was for
-- But it works
local function GetLength(Table: {QueueEntry}): number
	local Length = 0
	for Index, Value in Table do
		Length += 1
	end
	
	return Length
end

function ThreadQueue.New(TimeBetween: number?, MaxQueueLength: number?, EnableConcurrency: boolean?): ThreadQueue
	local self = {
		Queue = {},
		QueueRunning = false,
		TimeBetween = TimeBetween or 0,
		MaxQueueLength = MaxQueueLength or 75,
		EnableConcurrency = if EnableConcurrency then true else false,
	}
	return setmetatable(self, ThreadQueue)
end

function ThreadQueue.SubmitAsync(self: ThreadQueue, Callback: () -> any): (boolean, string?)
	if GetLength(self.Queue) > self.MaxQueueLength then
		return false, string.format("Queue is at capacity (%i)", self.MaxQueueLength)
	end

	local QueueEntry: QueueEntry = {
		Thread = coroutine.running(),
		Callback = Callback,
	}
	table.insert(self.Queue, QueueEntry)
	self:StartQueue()
	return coroutine.yield()
end

function ThreadQueue.SkipToLastEnqueued(self: ThreadQueue)
	if GetLength(self.Queue) > 1 then
		local LastEnqueuedEntry = self.Queue[GetLength(self.Queue)]
		self.Queue = { LastEnqueuedEntry }
	end
end

function ThreadQueue.StartQueue(self: ThreadQueue)
	task.defer(function()
		if self.QueueRunning then
			return
		end
		self.QueueRunning = true
		while GetLength(self.Queue) > 0 do
			self:PopQueueAsync()
			if self.TimeBetween > 0 then
				task.wait(self.TimeBetween)
			end
		end
		self.QueueRunning = false
	end)
end

function ThreadQueue.PopQueueAsync(self: ThreadQueue)
	local Entry = table.remove(self.Queue, 1)
	if not Entry then
		return
	end

	local function Execute()
		task.spawn(Entry.Thread, pcall(Entry.Callback))
	end

	if self.EnableConcurrency then
		task.spawn(Execute)
	else
		Execute()
	end
end

function ThreadQueue.ClearQueue(self: ThreadQueue)
	self.Queue = {}
	self.QueueRunning = false
end

return ThreadQueue
