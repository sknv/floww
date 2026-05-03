-- Функция для автоматического обновления updated_at таймстемпа
CREATE OR REPLACE FUNCTION floww_set_updated_at()
RETURNS trigger AS $$
BEGIN
  IF NEW IS DISTINCT FROM OLD THEN
    NEW.updated_at = now();
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--
-- Рабочие процессы
--

CREATE TYPE floww_workflow_status AS ENUM (
  'running', 'aborted', 'completed', 'failed'
);

CREATE TABLE IF NOT EXISTS floww_workflows (
  id                   uuid                  PRIMARY KEY DEFAULT uuidv7(),
  idempotency_key      uuid                  NOT NULL CONSTRAINT chk__floww_workflows__unique_idempotency_key UNIQUE,
  name                 text                  NOT NULL,
  status               floww_workflow_status NOT NULL DEFAULT 'running',
  input                bytea,
  output               bytea,
  priority             int                   NOT NULL DEFAULT 0,
  attempts             int                   NOT NULL DEFAULT 1,
  max_attempts         int                   NOT NULL DEFAULT 1,
  stuck_timeout_millis bigint                NOT NULL,
  completed_at         timestamptz,
  error_message        text,
  created_at           timestamptz           NOT NULL DEFAULT now(),
  updated_at           timestamptz           NOT NULL DEFAULT now()
);

COMMENT ON TABLE floww_workflows IS 'Рабочие процессы';
COMMENT ON COLUMN floww_workflows.idempotency_key IS 'Ключ идемпотентности';
COMMENT ON COLUMN floww_workflows.name IS 'Имя процесса';
COMMENT ON COLUMN floww_workflows.input IS 'Входящие параметры для процесса';
COMMENT ON COLUMN floww_workflows.output IS 'Результат работы процесса';
COMMENT ON COLUMN floww_workflows.priority IS 'Приоритет выполнения: чем больше, тем выше приоритет';
COMMENT ON COLUMN floww_workflows.attempts IS 'Текущее число попыток запуска процесса';
COMMENT ON COLUMN floww_workflows.max_attempts IS 'Максимальное число попыток запуска процесса';
COMMENT ON COLUMN floww_workflows.stuck_timeout_millis IS 'Промежуток времени в миллисекундах, после которого процесс считается зависшим';
COMMENT ON COLUMN floww_workflows.completed_at IS 'Время, в которое процесс был завершен';
COMMENT ON COLUMN floww_workflows.error_message IS 'Последнее сообщение об ошибке';

-- Триггер для автоматического обновления updated_at таймстемпа
CREATE TRIGGER trg__floww_workflows__updated_ata
BEFORE UPDATE ON floww_workflows
FOR EACH ROW
EXECUTE FUNCTION floww_set_updated_at();

-- Индекс для очистки завершенных задач
CREATE INDEX IF NOT EXISTS idx__floww_workflows__completed_cleaner
ON floww_workflows (created_at)
WHERE status IN ('aborted', 'completed');

-- Индекс для очистки мертвых задач
CREATE INDEX IF NOT EXISTS idx__floww_workflows__failed_cleaner
ON floww_workflows (created_at)
WHERE status = 'failed';

--
-- Задачи для возобновления рабочих процессов
--

CREATE TYPE floww_workflow_task_status AS ENUM (
  'pending', 'running', 'completed', 'failed'
);

CREATE TABLE IF NOT EXISTS floww_workflow_tasks (
  id                   uuid                       PRIMARY KEY DEFAULT uuidv7(),
  workflow_id          uuid                       NOT NULL REFERENCES floww_workflows (id) ON DELETE CASCADE,
  status               floww_workflow_task_status NOT NULL DEFAULT 'pending',
  priority             int                        NOT NULL DEFAULT 0,
  stuck_timeout_millis bigint                     NOT NULL,
  scheduled_at         timestamptz                NOT NULL DEFAULT now(),
  run_at               timestamptz,
  stuck_at             timestamptz,
  completed_at         timestamptz,
  created_at           timestamptz                NOT NULL DEFAULT now(),
  updated_at           timestamptz                NOT NULL DEFAULT now()
);

COMMENT ON TABLE floww_workflow_tasks IS 'Задачи для возобновления рабочих процессов';
COMMENT ON COLUMN floww_workflow_tasks.priority IS 'Приоритет выполнения: чем больше, тем выше приоритет';
COMMENT ON COLUMN floww_workflow_tasks.stuck_timeout_millis IS 'Промежуток времени в миллисекундах, после которого процесс считается зависшим';
COMMENT ON COLUMN floww_workflow_tasks.scheduled_at IS 'Время, в которое запланирован запуск задачи';
COMMENT ON COLUMN floww_workflow_tasks.run_at IS 'Время, в которое задача была запущена';
COMMENT ON COLUMN floww_workflow_tasks.stuck_at IS 'Время, после которого задача считается зависшей';
COMMENT ON COLUMN floww_workflow_tasks.completed_at IS 'Время, в которое задача была завершена';

-- Триггер для автоматического обновления updated_at таймстемпа
CREATE TRIGGER trg__floww_workflow_tasks__updated_at
BEFORE UPDATE ON floww_workflow_tasks
FOR EACH ROW
EXECUTE FUNCTION floww_set_updated_at();

-- Основной индекс для получения задач на выполнение
CREATE INDEX IF NOT EXISTS idx__floww_workflow_tasks__pending_worker
ON floww_workflow_tasks (priority DESC, scheduled_at)
WHERE status = 'pending';

-- Индекс для получения зависших задач
CREATE INDEX IF NOT EXISTS idx__floww_workflow_tasks__stuck_worker
ON floww_workflow_tasks (stuck_at)
WHERE status = 'running';

-- Индекс для поиска по внешнему ключу
CREATE INDEX IF NOT EXISTS idx__floww_workflow_tasks__workflow_id
ON floww_workflow_tasks (workflow_id);

--
-- Активности
--

CREATE TYPE floww_activity_status AS ENUM (
  'pending', 'running', 'completed', 'failed'
);

CREATE TABLE IF NOT EXISTS floww_activities (
  id                   uuid                  PRIMARY KEY DEFAULT uuidv7(),
  idempotency_key      uuid                  NOT NULL CONSTRAINT chk__floww_activities__unique_idempotency_key UNIQUE,
  workflow_id          uuid                  NOT NULL REFERENCES floww_workflows (id) ON DELETE CASCADE,
  name                 text                  NOT NULL,
  status               floww_activity_status NOT NULL DEFAULT 'pending',
  input                bytea,
  output               bytea,
  priority             int                   NOT NULL DEFAULT 0,
  attempts             int                   NOT NULL DEFAULT 0,
  max_attempts         int                   NOT NULL DEFAULT 1,
  stuck_timeout_millis bigint                NOT NULL,
  scheduled_at         timestamptz           NOT NULL DEFAULT now(),
  run_at               timestamptz,
  stuck_at             timestamptz,
  completed_at         timestamptz,
  error_message        text,
  created_at           timestamptz           NOT NULL DEFAULT now(),
  updated_at           timestamptz           NOT NULL DEFAULT now()
);

COMMENT ON TABLE floww_activities IS 'Очередь активностей';
COMMENT ON COLUMN floww_activities.idempotency_key IS 'Ключ идемпотентности';
COMMENT ON COLUMN floww_activities.name IS 'Имя активности';
COMMENT ON COLUMN floww_activities.input IS 'Входящие параметры для активности';
COMMENT ON COLUMN floww_activities.output IS 'Результат работы активности';
COMMENT ON COLUMN floww_activities.priority IS 'Приоритет выполнения: чем больше, тем выше приоритет';
COMMENT ON COLUMN floww_activities.attempts IS 'Текущее число попыток запуска задачи';
COMMENT ON COLUMN floww_activities.max_attempts IS 'Максимальное число попыток запуска задачи';
COMMENT ON COLUMN floww_activities.stuck_timeout_millis IS 'Промежуток времени в миллисекундах, после которого задача считается зависшей';
COMMENT ON COLUMN floww_activities.scheduled_at IS 'Время в которое запланирован запуск задачи';
COMMENT ON COLUMN floww_activities.run_at IS 'Время в которое задача была запущена';
COMMENT ON COLUMN floww_activities.stuck_at IS 'Время, после которого задача считается зависшей';
COMMENT ON COLUMN floww_activities.completed_at IS 'Время в которое задача была завершена';
COMMENT ON COLUMN floww_activities.error_message IS 'Последнее сообщение об ошибке';

-- Триггер для автоматического обновления updated_at таймстемпа
CREATE TRIGGER trg__floww_activities__updated_at
BEFORE UPDATE ON floww_activities
FOR EACH ROW
EXECUTE FUNCTION floww_set_updated_at();

-- Основной индекс для получения задач на выполнение
CREATE INDEX IF NOT EXISTS idx__floww_activities__pending_worker
ON floww_activities (priority DESC, scheduled_at)
WHERE status = 'pending';

-- Индекс для получения зависших задач
CREATE INDEX IF NOT EXISTS idx__floww_activities__stuck_worker
ON floww_activities (stuck_at)
WHERE status = 'running';

-- Индекс для поиска по внешнему ключу
CREATE INDEX IF NOT EXISTS idx__floww_activities__workflow_id_and_status
ON floww_activities (workflow_id, status);
