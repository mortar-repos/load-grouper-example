def run_script():
    import os
    from org.apache.pig.scripting import Pig

    # compile the pig code
    for i in range(10):
        print 'Run %s started!' % i
        P = Pig.compileFromFile("../pigscripts/avg_songs_per_split_counter.pig")

        bound = P.bind({"ITERATION_NUM":i})

        ps = bound.runSingle()
        print 'Run %s done!' % i

        result = ps.result("avg_split_song_count")
        for r in result.iterator():
            print r

        if int(r.get(1).toString()) >= 5:
            print 'Good enough! Quitting time!'
            break


if __name__ == '__main__':
    run_script()
