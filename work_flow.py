class basic_function:
    def checksum_is_ok(old_file, new_file):
        try:
            check_sum the file before upload.
            upload file.
            check_sum the file after upload.
            if old_check_sum == new_check_sum:
                return 1
            return 0
        except:
            raise error

    def upload_video(self):
        read static_db => choose 2 most-free nodes
        for each node:
            upload to this node.
            if checksum_is_ok => save self to mongodb:
                {film_id: xxx;
                 url: "/fbox/film_tinh_cam/hellomoto.mp4"
                 node: [node01 node02];
                 status: 1}

    def remove_is_ok(self):
        return (not os.is_file(self.url))

    def hard_remove(self):
        for node in self.on_node:
            remove from nodes.
            if remove_is_ok: continue
            else: return "cannot remove file from node xxx"
        return save to mongodb

    def copy_file(location1,location2):
        if location1 == location2:
            return "nothing to do"
        try:
            os.popen('rsync location1 location2')
            retry = 0
            if exist_status != 0 and retry < config['MAX_RETRY']:
                do retry
                retry += 1
        except:
            raise error


    def move_file(location1, location2):
        copy_file(location1,location2)

        if check_sum_is_ok:
            query mongodb => get old_film
            remove file from location1
            if remove_is_ok(location1) then update to mongodb(location1 => location2)
            else: 
                print "cannot remove old file, try to roll back"
                try remove file from location2
                except: print  "cannot remove new file, roll back fail"
                if remove_is_ok(location2): print "roll back successfully"


    def swap_file(vid_A,vid_B):
        try:
            move_video(vid_A from node_of_A to node_of_B)
        except Error, err:
            print "cannot move", str(err)

        try:
            move_video(vid_B from node_of_B to node_of_A)
        except:
            try:
               move_video(vid_A from node_of_A to node_of_B) ## (roll_back)
            except :
                print "cannot roll back"
            print "success roll back"



class controller_fuction:

    def relocate_file_based_on_traffic():
        '''This function runs daily or weekly, it determines the traffic utilized on each server, then relocate the hotest files to the sparest files'''
         read traffic database => calculate the average_traffic => accept_interval = [average_traffict - DELTA; average_traffic + DELTA]
         high traffic servers (higher the interval zone) = list_1
         low traffic servers (lower the average_traffic) = list_2
         for i in list_1:
             while i.traffic  still higher accept_interval:
                 get the list_of_hishest_view_count_videos of i (enough to make i return to accept_interval zone)
                 take file_K from  list_of_hishest_view_count_videos
                 for j in list_2:
                     while j.traffic still NOT_HIGHER (do not use lower, why??) accept_interval:
                         get the lowest_view_count_and_approximate_size_with(file_K) = file_G #it would be great if file_G = file_K, then we dont have to swap them, the onlything we have to do is to update database.
                         swap_file(file_K, file_G)
                         update i.traffic
                         update j.traffic
                         update list_of_hishest_view_count_videos


    def relocate_file_based_on_capacity():
        '''Determine the ratio used/free capacity on each server, which we call the ultilize_ratio, then relocate the rarely_used_file from high ultilize_ratio servers to the low ultilize_ratio servers.'''

        read the capacity database => calculate the average_ratio = (total ratio_of_each_server / total_server )
        calculate the accept_interval = [average_ratio - DELTA; average_ratio + DELTA]
        low ultilized_server (lower the accept zone) = list_1
        high ultilized_servers (higher the average_line)= list_2
        
        for i  in list_1:
            while i.capacity still lower the accept_zone:
                for j in list_2:
                    while j.capacity still NOT_LOWER accept_zone:
                        move_file(file_from_i; to_j)
                        update i.capacity
                        update list_1
                        update j.capacity
                        update list_2


    def disconnect_node(node_X):
        '''Disconnect a node from cluster'''
        data = query database 
        for i in data:
            if node_X in i.node:
                save to temporary_database: {file_path: position of node_X in list i.node}
                remove node_X from i.node 
                if there is no other node handle this i: choose the lowest_ultilize_node
                => save to database

        remove node_X from available_servers_list, so it wont count the traffic and capacity of node_X

    def reconnect_node(node_X):
        '''Reconnect a node from cluster'''
        data = query temporary_database
        for i in data:
            get the file_path and position => save to real_database.
        update node_C to available_servers_list, its traffic and capacity assign by the last day activation.


    def replicate_file(file_X):
        query file_X from database 
        choose the lowest_ultilize_node which is not in file_X.node
        copy_file(file_X,new_node)
        update database

    def replicate_node(node_X):
        data = query database
        for i in data:
            if node_X in i.node :
                replicate_file(i.path)


    def permanence_remove_node(node_X):
        replicate(node_X)
        disconnect_node(node_X)


















