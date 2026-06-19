defmodule Absinthe.Subscription.Local do
  @moduledoc """
  This module handles broadcasting documents that are local to this node
  """

  require Logger

  alias Absinthe.Pipeline.BatchResolver
  alias Absinthe.{Phase, Pipeline}

  # This module handles running and broadcasting documents that are local to this
  # node.

  @doc """
  Publish a mutation to the local node only.

  See also `Absinthe.Subscription.publish/3`
  """
  @spec publish_mutation(
          Absinthe.Subscription.Pubsub.t(),
          term,
          [Absinthe.Subscription.subscription_field_spec()]
        ) :: :ok
  def publish_mutation(pubsub, mutation_result, subscribed_fields) do
    {docs_and_topics, fields_by_topic} =
      Enum.reduce(subscribed_fields, {[], %{}}, fn {field, key_strategy},
                                                   {docs_and_topics, fields_by_topic} ->
        docs = get_docs_with_telemetry(pubsub, field, mutation_result, key_strategy)

        docs_and_topics_for_field =
          Enum.map(docs, fn {topic, doc} -> {topic, key_strategy, doc} end)

        fields_by_topic =
          Enum.reduce(docs, fields_by_topic, fn {topic, _doc}, fields_by_topic ->
            Map.put_new(fields_by_topic, topic, field)
          end)

        {docs_and_topics ++ docs_and_topics_for_field, fields_by_topic}
      end)

    run_docset_field = run_docset_field(fields_by_topic, subscribed_fields)

    run_docset_fn =
      if function_exported?(pubsub, :run_docset, 3) do
        &pubsub.run_docset/3
      else
        fn pubsub, docs_and_topics, mutation_result ->
          run_docset(pubsub, docs_and_topics, mutation_result, fields_by_topic)
        end
      end

    :telemetry.span(
      [:absinthe, :subscription, :local, :run_docset],
      %{
        run_docset_fn: run_docset_fn,
        mutation_result: mutation_result,
        docs_and_topics: docs_and_topics
      },
      fn ->
        {
          run_docset_fn.(pubsub, docs_and_topics, mutation_result),
          %{doc_count: doc_count(docs_and_topics)},
          field_metadata(run_docset_field)
        }
      end
    )

    :ok
  end

  defp get_docs_with_telemetry(pubsub, field, mutation_result, key_strategy) do
    :telemetry.span(
      [:absinthe, :subscription, :local, :get_docs],
      %{
        pubsub: pubsub,
        field: field,
        mutation_result: mutation_result,
        key_strategy: key_strategy
      },
      fn ->
        {docs, entries_scanned, doc_count} =
          get_docs(pubsub, field, mutation_result, key_strategy)

        {
          docs,
          %{entries_scanned: entries_scanned, doc_count: doc_count},
          field_metadata(field)
        }
      end
    )
  end

  defp run_docset(pubsub, docs_and_topics, mutation_result, fields_by_topic) do
    for {topic, key_strategy, doc} <- docs_and_topics do
      try do
        pipeline = pipeline(doc, mutation_result)
        field = Map.get(fields_by_topic, topic)

        {:ok, %{result: data}, _} = Absinthe.Pipeline.run(doc.source, pipeline)

        Logger.debug("""
        Absinthe Subscription Publication
        Field Topic: #{inspect(key_strategy)}
        Subscription id: #{inspect(topic)}
        Data: #{inspect(data)}
        """)

        :ok = dispatch_with_telemetry(pubsub, field, topic, data)
      rescue
        e ->
          BatchResolver.pipeline_error(e, __STACKTRACE__)
      end
    end
  end

  def pipeline(doc, mutation_result) do
    pipeline =
      doc.initial_phases
      |> Pipeline.replace(
        Phase.Telemetry,
        {Phase.Telemetry, event: [:subscription, :publish, :start]}
      )
      |> Pipeline.without(Phase.Subscription.SubscribeSelf)
      |> Pipeline.insert_before(
        Phase.Document.Execution.Resolution,
        {Phase.Document.OverrideRoot, root_value: mutation_result}
      )
      |> Pipeline.upto(Phase.Document.Execution.Resolution)

    pipeline = [
      pipeline,
      [
        result_phase(doc),
        {Absinthe.Phase.Telemetry, event: [:subscription, :publish, :stop]}
      ]
    ]

    pipeline
  end

  defp get_docs(pubsub, field, mutation_result, topic: topic_fun)
       when is_function(topic_fun, 1) do
    do_get_docs(pubsub, field, topic_fun.(mutation_result))
  end

  defp get_docs(pubsub, field, _mutation_result, key) do
    do_get_docs(pubsub, field, key)
  end

  defp do_get_docs(pubsub, field, keys) do
    {docs, entries_scanned, doc_ids} =
      keys
      |> List.wrap()
      |> Enum.map(&to_string/1)
      |> Enum.reduce({[], 0, MapSet.new()}, fn key, {docs, entries_scanned, doc_ids} ->
        {key_docs, entry_count} = Absinthe.Subscription.get_with_entry_count(pubsub, {field, key})

        doc_ids =
          Enum.reduce(key_docs, doc_ids, fn {doc_id, _doc}, doc_ids ->
            MapSet.put(doc_ids, doc_id)
          end)

        {docs ++ Enum.to_list(key_docs), entries_scanned + entry_count, doc_ids}
      end)

    {docs, entries_scanned, MapSet.size(doc_ids)}
  end

  defp dispatch_with_telemetry(pubsub, field, topic, data) do
    subscriber_count = subscriber_count(pubsub, topic)

    :telemetry.span(
      [:absinthe, :subscription, :local, :dispatch],
      field_metadata(field),
      fn ->
        {
          pubsub.publish_subscription(topic, data),
          %{subscriber_count: subscriber_count},
          field_metadata(field)
        }
      end
    )
  end

  defp subscriber_count(pubsub, topic) do
    pubsub
    |> Absinthe.Subscription.registry_name()
    |> Registry.lookup(topic)
    |> length()
  end

  defp doc_count(docs_and_topics) do
    docs_and_topics
    |> Enum.map(fn {topic, _key_strategy, _doc} -> topic end)
    |> MapSet.new()
    |> MapSet.size()
  end

  defp run_docset_field(fields_by_topic, subscribed_fields) do
    case available_field(Map.values(fields_by_topic)) do
      nil -> available_field(Keyword.keys(subscribed_fields))
      field -> field
    end
  end

  defp available_field(fields) do
    fields
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
    |> case do
      [field] -> field
      _ -> nil
    end
  end

  defp field_metadata(nil), do: %{}
  defp field_metadata(field), do: %{field: field}

  defp result_phase(doc) do
    # use the configured result phase from the initial pipeline
    # this will allow the result of the subscription data to match
    # the output of query/mutation. An example of result phase is
    # Absinthe.Phoenix.Controller.Result where the output will have
    # atom keys and allow struct to be returned

    doc.initial_phases
    |> Pipeline.from(Phase.Blueprint)
    |> case do
      [{Phase.Blueprint, opts} | _] ->
        Keyword.get(opts, :result_phase, Phase.Document.Result)

      _ ->
        Phase.Document.Result
    end
  end
end
