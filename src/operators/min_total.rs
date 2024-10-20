use std::collections::BTreeMap;

use differential_dataflow::difference::{IsZero, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{ArrangeByKey, Arranged};
use differential_dataflow::trace::cursor::IntoOwned;
use differential_dataflow::trace::{BatchReader, Cursor, TraceReader};
use differential_dataflow::{AsCollection, Collection, ExchangeData, Hashable};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Capability, Operator};
use timely::dataflow::*;
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

pub trait MinTotal<G: Scope, K: ExchangeData, V: ExchangeData>
where
    G::Timestamp: TotalOrder + Lattice + Ord,
{
    fn min_total(&self) -> Collection<G, (K, V), isize> {
        self.min_total_core()
    }

    fn min_total_core<R2: Clone + From<i8> + 'static>(&self) -> Collection<G, (K, V), R2>;
}

impl<G: Scope, K: ExchangeData + Hashable, V: ExchangeData, R: ExchangeData + Semigroup>
    MinTotal<G, K, V> for Collection<G, (K, V), R>
where
    G::Timestamp: TotalOrder + Lattice + Ord,
{
    fn min_total_core<R2: Clone + From<i8> + 'static>(&self) -> Collection<G, (K, V), R2> {
        self.arrange_by_key_named("Arrange: MinTotal")
            .min_total_core()
    }
}

impl<G, K, V, Tr> MinTotal<G, K, V> for Arranged<G, Tr>
where
    G: Scope<Timestamp = Tr::Time>,
    Tr: for<'a> TraceReader + Clone + 'static,
    for<'a> Tr::Key<'a>: IntoOwned<'a, Owned = K>,
    for<'a> Tr::Val<'a>: IntoOwned<'a, Owned = V> + Ord,
    for<'a> Tr::Diff: Semigroup<Tr::DiffGat<'a>>,
    K: ExchangeData,
    V: ExchangeData,
    Tr::Time: TotalOrder,
{
    fn min_total_core<R2: Clone + From<i8> + 'static>(&self) -> Collection<G, (K, V), R2> {
        let mut trace = self.trace.clone();
        let mut buffer = Vec::new();

        self.stream
            .unary_frontier(Pipeline, "MinTotal", move |_, _| {
                // tracks the upper limit of known-complete timestamps.
                let mut lower_limit = Antichain::from_elem(G::Timestamp::minimum());
                let mut upper_limit = Antichain::from_elem(G::Timestamp::minimum());

                move |input, output| {
                    let mut capability: Option<Capability<Tr::Time>> = None;
                    let mut batch_storage = vec![];
                    let mut pending: BTreeMap<
                        Tr::Key<'_>,
                        BTreeMap<Tr::Time, Vec<(Tr::Val<'_>, Tr::Diff)>>,
                    > = Default::default();

                    input.for_each(|cap, batches| {
                        batches.swap(&mut buffer);
                        for batch in buffer.drain(..) {
                            upper_limit.clone_from(&batch.upper());
                            batch_storage.push(batch);
                        }

                        // single capability is enough since we are in total time scope and the batches are in order.
                        if let Some(capability) = &capability {
                            assert!(capability.time().less_equal(cap.time()));
                        } else {
                            capability = Some(cap.retain());
                        }
                    });

                    for batch in &batch_storage {
                        let mut batch_cursor = batch.cursor();
                        while let Some(key) = batch_cursor.get_key(&batch) {
                            let values = pending.entry(key).or_default();
                            while let Some(val) = batch_cursor.get_val(&batch) {
                                batch_cursor.map_times(&batch, |time, diff| {
                                    let values_diffs = values.entry(time.into_owned()).or_default();
                                    values_diffs.push((val, diff.into_owned()));
                                });
                                batch_cursor.step_val(&batch);
                            }
                            batch_cursor.step_key(&batch);
                        }
                    }

                    if let Some(capability) = capability {
                        let mut session = output.session(&capability);
                        let (mut trace_cursor, trace_storage) =
                            trace.cursor_through(lower_limit.borrow()).unwrap();

                        // per key computation
                        for (key, values) in pending {
                            trace_cursor.seek_key(&trace_storage, key);
                            let mut trace_head_value =
                                if trace_cursor.get_key(&trace_storage) == Some(key) {
                                    current_trace_value::<Tr>(&mut trace_cursor, &trace_storage)
                                } else {
                                    None
                                };
                            let mut prev_min_value = trace_head_value.as_ref().map(|x| x.0);

                            // per key/time computation
                            let mut batch_values: BTreeMap<Tr::Val<'_>, Tr::Diff> = BTreeMap::new();
                            for (time, values) in values {
                                for (value, diff) in values {
                                    batch_values
                                        .entry(value)
                                        .and_modify(|c| {
                                            c.plus_equals(&Tr::DiffGat::borrow_as(&diff))
                                        })
                                        .or_insert(diff);
                                }

                                batch_values.retain(|_, v| !v.is_zero());

                                // calculate current min value
                                let current_min_value = loop {
                                    let batch_min = batch_values.first_key_value();
                                    match (&trace_head_value, batch_min) {
                                        (Some((v1, d1)), Some((v2, d2))) => {
                                            if v1 > v2 {
                                                break Some(*v2);
                                            } else if v1 == v2 {
                                                let mut diff = d1.clone();
                                                diff.plus_equals(&Tr::DiffGat::borrow_as(&d2));
                                                if diff.is_zero() {
                                                    batch_values.pop_first();
                                                    trace_cursor.step_val(&trace_storage);
                                                    trace_head_value = current_trace_value::<Tr>(
                                                        &mut trace_cursor,
                                                        &trace_storage,
                                                    );
                                                } else {
                                                    break Some(*v1);
                                                }
                                            } else {
                                                break Some(*v1);
                                            }
                                        }
                                        (Some((v1, _)), None) => break Some(*v1),
                                        (None, Some((v2, _))) => break Some(*v2),
                                        (None, None) => break None,
                                    }
                                };

                                if prev_min_value != current_min_value {
                                    if let Some(v) = &prev_min_value {
                                        session.give((
                                            (key.into_owned(), (*v).into_owned()),
                                            time.clone(),
                                            R2::from(-1i8),
                                        ));
                                    }
                                    if let Some(v) = &current_min_value {
                                        session.give((
                                            (key.into_owned(), (*v).into_owned()),
                                            time,
                                            R2::from(1i8),
                                        ));
                                    }
                                    prev_min_value = current_min_value
                                }
                            }
                        }
                    }

                    // tidy up the shared input trace.
                    trace.advance_upper(&mut upper_limit);
                    lower_limit.clone_from(&upper_limit);
                    trace.set_logical_compaction(upper_limit.borrow());
                    trace.set_physical_compaction(upper_limit.borrow());
                }
            })
            .as_collection()
    }
}

fn current_trace_value<'trace, Tr>(
    trace_cursor: &mut Tr::Cursor,
    trace_storage: &'trace Tr::Storage,
) -> Option<(Tr::Val<'trace>, Tr::Diff)>
where
    Tr: for<'a> TraceReader + Clone + 'static,
    for<'a> Tr::Diff: Semigroup<Tr::DiffGat<'a>>,
{
    let mut ret: Option<(Tr::Val<'trace>, Tr::Diff)> = None;
    while let Some(val) = trace_cursor.get_val(&trace_storage) {
        trace_cursor.map_times(&trace_storage, |_, diff| {
            if let Some((_, c)) = &mut ret {
                c.plus_equals(&diff);
            } else {
                ret = Some((val, diff.into_owned()))
            }
        });

        if let Some((_, c)) = &ret {
            if !c.is_zero() {
                break;
            }
        }
        ret = None;
        trace_cursor.step_val(&trace_storage);
    }

    ret
}

#[cfg(test)]
mod tests {
    use differential_dataflow::input::Input;
    use differential_dataflow::operators::arrange::ArrangeByKey;
    use timely::dataflow::ProbeHandle;

    use crate::operators::min_total::MinTotal;

    #[test]
    fn test_min_total() {
        timely::execute_directly(move |worker| {
            let mut probe = ProbeHandle::new();
            let (mut input, _) = worker.dataflow::<u32, _, _>(|scope| {
                let (handle, input) = scope.new_collection();
                let arrange = input.arrange_by_key();
                arrange
                    .min_total()
                    .inspect(|x| println!("{:?}", x))
                    .probe_with(&mut probe);
                (handle, arrange.trace)
            });

            println!("/////////// round1");
            input.insert(("a".to_string(), 1));
            input.insert(("a".to_string(), 2));
            input.advance_to(input.time() + 1);
            input.flush();
            worker.step_while(|| probe.less_than(input.time()));

            println!("\n/////////// round2");
            input.update(("a".to_string(), 1), -1);
            input.advance_to(input.time() + 1);
            input.flush();
            worker.step_while(|| probe.less_than(input.time()));

            println!("\n/////////// round3");
            input.update(("a".to_string(), 2), -1);
            input.advance_to(input.time() + 1);
            input.flush();
            worker.step_while(|| probe.less_than(input.time()));

            println!("\n/////////// round3");
            input.update(("a".to_string(), 1), -1);
            input.advance_to(input.time() + 1);
            input.flush();
            worker.step_while(|| probe.less_than(input.time()));

            println!("\n/////////// round3");
            input.update(("a".to_string(), 1), 1);
            input.advance_to(input.time() + 1);
            input.flush();
            worker.step_while(|| probe.less_than(input.time()));

            println!("\n/////////// round3");
            input.update(("a".to_string(), 10), 1);
            input.advance_to(input.time() + 1);
            input.flush();
            worker.step_while(|| probe.less_than(input.time()));

            println!("\n/////////// round3");
            input.update(("a".to_string(), 9), 1);
            input.advance_to(input.time() + 1);
            input.flush();
            worker.step_while(|| probe.less_than(input.time()));

            println!("\n/////////// round3");
            input.update(("a".to_string(), 9), -1);
            input.advance_to(input.time() + 1);
            input.flush();
            worker.step_while(|| probe.less_than(input.time()));
        });
    }
}
