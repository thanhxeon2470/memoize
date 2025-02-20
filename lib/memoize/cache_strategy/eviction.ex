defmodule Memoize.CacheStrategy.Eviction do
  @behaviour Memoize.CacheStrategy

  @ets_tab __MODULE__
  @read_history_tab Module.concat(__MODULE__, "ReadHistory")
  @expiration_tab Module.concat(__MODULE__, "Expiration")

  defp max_threshold() do
    Memoize.Config.opts().max_threshold
  end

  defp min_threshold() do
    Memoize.Config.opts().min_threshold
  end

  def init(opts) do
    :ets.new(@ets_tab, [:public, :set, :named_table, {:read_concurrency, true}])
    :ets.new(@read_history_tab, [:public, :set, :named_table, {:write_concurrency, true}])
    :ets.new(@expiration_tab, [:public, :ordered_set, :named_table])

    app_opts = Application.fetch_env!(:memoize, __MODULE__)
    max_threshold = opts[:max_threshold] || Keyword.fetch!(app_opts, :max_threshold)

    opts = Keyword.put(opts, :max_threshold, max_threshold)

    opts =
      if max_threshold == :infinity do
        opts
      else
        Keyword.put(
          opts,
          :min_threshold,
          opts[:min_threshold] || Keyword.fetch!(app_opts, :min_threshold)
        )
      end

    opts
    |> Memoize.CacheStrategy.Eviction.Persistent.init()
  end

  def tab(_key) do
    @ets_tab
  end

  def used_bytes() do
    words = 0
    words = words + :ets.info(@ets_tab, :memory)
    words = words + :ets.info(@read_history_tab, :memory)

    words * :erlang.system_info(:wordsize)
  end

  def cache(key, value, opts) do
    if max_threshold() == :infinity do
      do_cache(key, value, opts)
    else
      if used_bytes() > max_threshold() do
        garbage_collect()
      end

      do_cache(key, value, opts)
    end
  end

  defp do_cache(key, _value, opts) do
    case Keyword.fetch(opts, :expires_in) do
      {:ok, expires_in} ->
        expired_at = System.monotonic_time(:millisecond) + expires_in
        counter = System.unique_integer()
        :ets.insert(@expiration_tab, {{expired_at, counter}, key})

      :error ->
        :ok
    end

    %{permanent: Keyword.get(opts, :permanent, false)}
  end

  def read(key, _value, context) do
    expired? = clear_expired_cache(key)

    unless context.permanent do
      counter = System.unique_integer([:monotonic, :positive])
      :ets.insert(@read_history_tab, {key, counter})
    end

    if expired?, do: :retry, else: :ok
  end

  def invalidate() do
    num_deleted = :ets.select_delete(@ets_tab, [{{:_, {:completed, :_, :_}}, [], [true]}])
    :ets.delete_all_objects(@read_history_tab)
    num_deleted
  end

  def invalidate(key) do
    num_deleted = :ets.select_delete(@ets_tab, [{{key, {:completed, :_, :_}}, [], [true]}])
    :ets.select_delete(@read_history_tab, [{{key, :_}, [], [true]}])
    num_deleted
  end

  def garbage_collect() do
    if max_threshold() == :infinity do
      0
    else
      if used_bytes() <= min_threshold() do
        # still don't collect
        0
      else
        # remove values ordered by last accessed time until used bytes less than min_threshold().
        values = :lists.keysort(2, :ets.tab2list(@read_history_tab))
        stream = values |> Stream.filter(fn n -> n != :permanent end) |> Stream.with_index(1)

        try do
          for {{key, _}, num_deleted} <- stream do
            :ets.select_delete(@ets_tab, [{{key, {:completed, :_, :_}}, [], [true]}])
            :ets.delete(@read_history_tab, key)

            if used_bytes() <= min_threshold() do
              throw({:break, num_deleted})
            end
          end
        else
          _ -> length(values)
        catch
          {:break, num_deleted} -> num_deleted
        end
      end
    end
  end

  def clear_expired_cache(read_key \\ nil, expired? \\ false) do
    case :ets.first(@expiration_tab) do
      :"$end_of_table" ->
        expired?

      {expired_at, _counter} = key ->
        case :ets.lookup(@expiration_tab, key) do
          [] ->
            # retry
            clear_expired_cache(read_key, expired?)

          [{^key, cache_key}] ->
            now = System.monotonic_time(:millisecond)

            if now > expired_at do
              invalidate(cache_key)
              :ets.delete(@expiration_tab, key)
              expired? = expired? || cache_key == read_key
              # next
              clear_expired_cache(read_key, expired?)
            else
              # completed
              expired?
            end
        end
    end
  end
  def persistent() do
    __MODULE__.Persistent
  end
end
